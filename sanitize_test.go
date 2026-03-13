package main

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseSanitizeConfig_Inline(t *testing.T) {
	cfg, err := parseSanitizeConfig("email=email,name=firstName,phone=phone")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := map[string]string{
		"email": "email",
		"name":  "firstName",
		"phone": "phone",
	}
	for k, v := range want {
		if cfg.Fields[k] != v {
			t.Errorf("Fields[%q] = %q, want %q", k, cfg.Fields[k], v)
		}
	}
	if len(cfg.Fields) != len(want) {
		t.Errorf("got %d fields, want %d", len(cfg.Fields), len(want))
	}
}

func TestParseSanitizeConfig_YAML(t *testing.T) {
	yamlContent := `fields:
  email: email
  first_name: firstName
  company: companyName
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("writing temp yaml: %v", err)
	}

	cfg, err := parseSanitizeConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := map[string]string{
		"email":      "email",
		"first_name": "firstName",
		"company":    "companyName",
	}
	for k, v := range want {
		if cfg.Fields[k] != v {
			t.Errorf("Fields[%q] = %q, want %q", k, cfg.Fields[k], v)
		}
	}
}

func TestParseSanitizeConfig_YML(t *testing.T) {
	yamlContent := `fields:
  email: email
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	if err := os.WriteFile(path, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("writing temp yml: %v", err)
	}

	cfg, err := parseSanitizeConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Fields["email"] != "email" {
		t.Errorf("Fields[email] = %q, want %q", cfg.Fields["email"], "email")
	}
}

func TestParseSanitizeConfig_Malformed(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"missing equals", "emailemail"},
		{"empty key", "=email"},
		{"empty value", "email="},
		{"empty string", ""},
		{"only commas", ",,,"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseSanitizeConfig(tt.input)
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

func TestParseSanitizeConfig_UnknownFakerType(t *testing.T) {
	_, err := parseSanitizeConfig("email=email,name=bogusType")
	if err == nil {
		t.Fatal("expected error for unknown faker type, got nil")
	}
}

func TestParseSanitizeConfig_YAMLFileNotFound(t *testing.T) {
	_, err := parseSanitizeConfig("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestSanitizeRecord_FlatMap(t *testing.T) {
	cfg := sanitizeConfig{Fields: map[string]string{
		"email": "email",
		"name":  "firstName",
	}}
	s := newSanitizer(cfg, 42)

	data := map[string]any{
		"email":   "real@example.com",
		"name":    "Alice",
		"age":     30,
		"country": "US",
	}
	s.sanitizeRecord(data)

	if data["email"] == "real@example.com" {
		t.Error("email should have been replaced")
	}
	if data["name"] == "Alice" {
		t.Error("name should have been replaced")
	}
	if data["age"] != 30 {
		t.Error("age should be untouched")
	}
	if data["country"] != "US" {
		t.Error("country should be untouched")
	}
}

func TestSanitizeRecord_NestedMap(t *testing.T) {
	cfg := sanitizeConfig{Fields: map[string]string{
		"email": "email",
	}}
	s := newSanitizer(cfg, 42)

	data := map[string]any{
		"profile": map[string]any{
			"email": "nested@example.com",
			"bio":   "some bio",
		},
	}
	s.sanitizeRecord(data)

	profile := data["profile"].(map[string]any)
	if profile["email"] == "nested@example.com" {
		t.Error("nested email should have been replaced")
	}
	if profile["bio"] != "some bio" {
		t.Error("bio should be untouched")
	}
}

func TestSanitizeRecord_ArrayOfMaps(t *testing.T) {
	cfg := sanitizeConfig{Fields: map[string]string{
		"name": "firstName",
	}}
	s := newSanitizer(cfg, 42)

	data := map[string]any{
		"contacts": []any{
			map[string]any{"name": "Bob", "role": "admin"},
			map[string]any{"name": "Carol", "role": "user"},
		},
	}
	s.sanitizeRecord(data)

	contacts := data["contacts"].([]any)
	c0 := contacts[0].(map[string]any)
	c1 := contacts[1].(map[string]any)
	if c0["name"] == "Bob" {
		t.Error("first contact name should have been replaced")
	}
	if c1["name"] == "Carol" {
		t.Error("second contact name should have been replaced")
	}
	if c0["role"] != "admin" || c1["role"] != "user" {
		t.Error("roles should be untouched")
	}
}

func TestSanitizeRecord_NonStringValues(t *testing.T) {
	cfg := sanitizeConfig{Fields: map[string]string{
		"email": "email",
		"count": "firstName",
		"active": "lastName",
	}}
	s := newSanitizer(cfg, 42)

	data := map[string]any{
		"email":  "real@example.com",
		"count":  42,
		"active": true,
	}
	s.sanitizeRecord(data)

	// email is a string, should be replaced
	if data["email"] == "real@example.com" {
		t.Error("email should have been replaced")
	}
	// non-string values should be left as-is
	if data["count"] != 42 {
		t.Errorf("count should be untouched, got %v", data["count"])
	}
	if data["active"] != true {
		t.Errorf("active should be untouched, got %v", data["active"])
	}
}

func TestSanitizer_SeedDeterminism(t *testing.T) {
	cfg := sanitizeConfig{Fields: map[string]string{
		"email": "email",
		"name":  "firstName",
	}}

	s1 := newSanitizer(cfg, 123)
	s2 := newSanitizer(cfg, 123)

	data1 := map[string]any{"email": "a@b.com", "name": "X"}
	data2 := map[string]any{"email": "a@b.com", "name": "X"}

	s1.sanitizeRecord(data1)
	s2.sanitizeRecord(data2)

	if data1["email"] != data2["email"] {
		t.Errorf("same seed should produce same email: %q vs %q", data1["email"], data2["email"])
	}
	if data1["name"] != data2["name"] {
		t.Errorf("same seed should produce same name: %q vs %q", data1["name"], data2["name"])
	}
}

func TestSanitizer_SeedZeroRandomness(t *testing.T) {
	cfg := sanitizeConfig{Fields: map[string]string{
		"email": "email",
	}}

	// Generate multiple values from two seed=0 sanitizers
	// and check that at least one pair differs.
	const n = 20
	results1 := make([]string, n)
	results2 := make([]string, n)

	s1 := newSanitizer(cfg, 0)
	s2 := newSanitizer(cfg, 0)

	for i := range n {
		d1 := map[string]any{"email": "x@y.com"}
		d2 := map[string]any{"email": "x@y.com"}
		s1.sanitizeRecord(d1)
		s2.sanitizeRecord(d2)
		results1[i] = d1["email"].(string)
		results2[i] = d2["email"].(string)
	}

	anyDifferent := false
	for i := range n {
		if results1[i] != results2[i] {
			anyDifferent = true
			break
		}
	}
	if !anyDifferent {
		t.Error("seed=0 should produce different output across sanitizer instances (probabilistic check)")
	}
}

func TestRunSanitize(t *testing.T) {
	// Create input CSV
	inputDir := t.TempDir()
	outputDir := filepath.Join(t.TempDir(), "out")

	csvContent := "__path__,email,name,age\nusers/alice,alice@real.com,Alice,30\nusers/bob,bob@real.com,Bob,25\n"
	if err := os.WriteFile(filepath.Join(inputDir, "users.csv"), []byte(csvContent), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := sanitizeConfig{Fields: map[string]string{
		"email": "email",
		"name":  "firstName",
	}}

	if err := runSanitize(cfg, inputDir, outputDir, 42); err != nil {
		t.Fatalf("runSanitize error: %v", err)
	}

	// Read output CSV
	outData, err := os.ReadFile(filepath.Join(outputDir, "users.csv"))
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}

	reader := csv.NewReader(strings.NewReader(string(outData)))
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("parsing output CSV: %v", err)
	}

	// Header should be preserved
	if rows[0][0] != "__path__" || rows[0][1] != "email" || rows[0][2] != "name" || rows[0][3] != "age" {
		t.Errorf("headers changed: %v", rows[0])
	}

	// __path__ should be untouched
	if rows[1][0] != "users/alice" {
		t.Errorf("__path__ should be untouched, got %q", rows[1][0])
	}

	// email and name should be replaced
	if rows[1][1] == "alice@real.com" {
		t.Error("email should have been replaced")
	}
	if rows[1][2] == "Alice" {
		t.Error("name should have been replaced")
	}

	// age should be untouched (not in config)
	if rows[1][3] != "30" {
		t.Errorf("age should be untouched, got %q", rows[1][3])
	}
}

func TestRunSanitize_PreservesDirectoryStructure(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := filepath.Join(t.TempDir(), "out")

	// Create nested structure: users.csv and users/orders.csv
	if err := os.MkdirAll(filepath.Join(inputDir, "users"), 0755); err != nil {
		t.Fatal(err)
	}
	csv1 := "__path__,email\nusers/alice,alice@example.com\n"
	csv2 := "__path__,email\nusers/alice/orders/o1,order@example.com\n"
	if err := os.WriteFile(filepath.Join(inputDir, "users.csv"), []byte(csv1), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "users", "orders.csv"), []byte(csv2), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := sanitizeConfig{Fields: map[string]string{"email": "email"}}
	if err := runSanitize(cfg, inputDir, outputDir, 42); err != nil {
		t.Fatalf("runSanitize error: %v", err)
	}

	// Both files should exist in output with same structure
	if _, err := os.Stat(filepath.Join(outputDir, "users.csv")); err != nil {
		t.Errorf("expected users.csv in output: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outputDir, "users", "orders.csv")); err != nil {
		t.Errorf("expected users/orders.csv in output: %v", err)
	}
}

func TestRunSanitize_SeedDeterminism(t *testing.T) {
	inputDir := t.TempDir()
	outDir1 := filepath.Join(t.TempDir(), "out1")
	outDir2 := filepath.Join(t.TempDir(), "out2")

	csvContent := "__path__,email,name\nusers/alice,alice@real.com,Alice\n"
	if err := os.WriteFile(filepath.Join(inputDir, "users.csv"), []byte(csvContent), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := sanitizeConfig{Fields: map[string]string{"email": "email", "name": "firstName"}}

	if err := runSanitize(cfg, inputDir, outDir1, 99); err != nil {
		t.Fatal(err)
	}
	if err := runSanitize(cfg, inputDir, outDir2, 99); err != nil {
		t.Fatal(err)
	}

	data1, _ := os.ReadFile(filepath.Join(outDir1, "users.csv"))
	data2, _ := os.ReadFile(filepath.Join(outDir2, "users.csv"))

	if string(data1) != string(data2) {
		t.Errorf("same seed should produce identical output:\n%s\nvs\n%s", data1, data2)
	}
}

func TestRunSanitize_SkipsSpecialColumns(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := filepath.Join(t.TempDir(), "out")

	// __fs_types__ column should never be sanitized even if field name matches config
	csvContent := "__path__,email,__fs_types__\nusers/alice,alice@example.com,\"{\"\"email\"\":\"\"string\"\"}\"\n"
	if err := os.WriteFile(filepath.Join(inputDir, "data.csv"), []byte(csvContent), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := sanitizeConfig{Fields: map[string]string{"email": "email"}}
	if err := runSanitize(cfg, inputDir, outputDir, 42); err != nil {
		t.Fatal(err)
	}

	outData, _ := os.ReadFile(filepath.Join(outputDir, "data.csv"))
	reader := csv.NewReader(strings.NewReader(string(outData)))
	rows, _ := reader.ReadAll()

	// __path__ untouched
	if rows[1][0] != "users/alice" {
		t.Errorf("__path__ should be untouched, got %q", rows[1][0])
	}
	// email should be replaced
	if rows[1][1] == "alice@example.com" {
		t.Error("email should have been replaced")
	}
	// __fs_types__ untouched
	if rows[1][2] != "{\"email\":\"string\"}" {
		t.Errorf("__fs_types__ should be untouched, got %q", rows[1][2])
	}
}
