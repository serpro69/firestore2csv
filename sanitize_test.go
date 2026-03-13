package main

import (
	"os"
	"path/filepath"
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
