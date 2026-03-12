package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/spf13/cobra"
	"google.golang.org/genproto/googleapis/type/latlng"
)

func TestDocumentPath(t *testing.T) {
	client, err := firestore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Skipf("cannot create Firestore client: %v", err)
	}
	defer client.Close()

	tests := []struct {
		ref  *firestore.DocumentRef
		want string
	}{
		{client.Doc("users/alice"), "users/alice"},
		{client.Doc("users/alice/orders/order1"), "users/alice/orders/order1"},
		{client.Collection("top").Doc("doc"), "top/doc"},
	}
	for _, tt := range tests {
		got := documentPath(tt.ref)
		if got != tt.want {
			t.Errorf("documentPath(%q) = %q, want %q", tt.ref.Path, got, tt.want)
		}
	}
}

func TestFmtInt(t *testing.T) {
	tests := []struct {
		input int
		want  string
	}{
		{0, "0"},
		{1, "1"},
		{42, "42"},
		{999, "999"},
		{1000, "1,000"},
		{10000, "10,000"},
		{100000, "100,000"},
		{1000000, "1,000,000"},
		{1234567, "1,234,567"},
	}
	for _, tt := range tests {
		if got := fmtInt(tt.input); got != tt.want {
			t.Errorf("fmtInt(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestFormatValue(t *testing.T) {
	fixedTime := time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC)

	tests := []struct {
		name  string
		input any
		want  string
	}{
		{"nil", nil, ""},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"int64", int64(42), "42"},
		{"int64 negative", int64(-100), "-100"},
		{"float64", float64(3.14), "3.14"},
		{"float64 integer", float64(10), "10"},
		{"string", "hello world", "hello world"},
		{"empty string", "", ""},
		{"time", fixedTime, fixedTime.Format(time.RFC3339Nano)},
		{"latlng", &latlng.LatLng{Latitude: 37.7749, Longitude: -122.4194}, `{"lat":37.7749,"lng":-122.4194}`},
		{"bytes", []byte("hello"), base64.StdEncoding.EncodeToString([]byte("hello"))},
		{"empty bytes", []byte{}, ""},
		{"array", []any{"a", int64(1)}, `["a",1]`},
		{"empty array", []any{}, `[]`},
		{"map", map[string]any{"key": "value"}, `{"key":"value"}`},
		{"nested map", map[string]any{
			"inner": map[string]any{"a": int64(1)},
		}, `{"inner":{"a":1}}`},
		{"unknown type", struct{ X int }{42}, "{42}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatValue(tt.input)
			if got != tt.want {
				t.Errorf("formatValue(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestConvertForJSON(t *testing.T) {
	fixedTime := time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC)

	tests := []struct {
		name  string
		input any
		want  any
	}{
		{"nil", nil, nil},
		{"bool", true, true},
		{"int64", int64(42), int64(42)},
		{"float64", float64(3.14), float64(3.14)},
		{"string", "hello", "hello"},
		{"time", fixedTime, fixedTime.Format(time.RFC3339Nano)},
		{"latlng", &latlng.LatLng{Latitude: 1.0, Longitude: 2.0}, map[string]float64{"lat": 1.0, "lng": 2.0}},
		{"bytes", []byte("abc"), base64.StdEncoding.EncodeToString([]byte("abc"))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertForJSON(tt.input)
			switch w := tt.want.(type) {
			case nil:
				if got != nil {
					t.Errorf("convertForJSON(%v) = %v, want nil", tt.input, got)
				}
			case map[string]float64:
				gm, ok := got.(map[string]float64)
				if !ok {
					t.Fatalf("convertForJSON(%v) type = %T, want map[string]float64", tt.input, got)
				}
				for k, v := range w {
					if gm[k] != v {
						t.Errorf("convertForJSON(%v)[%q] = %v, want %v", tt.input, k, gm[k], v)
					}
				}
			default:
				if got != tt.want {
					t.Errorf("convertForJSON(%v) = %v (%T), want %v (%T)", tt.input, got, got, tt.want, tt.want)
				}
			}
		})
	}
}

func TestConvertForJSON_Array(t *testing.T) {
	input := []any{"a", int64(1), true}
	got := convertForJSON(input)
	arr, ok := got.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", got)
	}
	if len(arr) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(arr))
	}
	if arr[0] != "a" || arr[1] != int64(1) || arr[2] != true {
		t.Errorf("unexpected array contents: %v", arr)
	}
}

func TestConvertForJSON_Map(t *testing.T) {
	input := map[string]any{"key": "value", "num": int64(42)}
	got := convertForJSON(input)
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", got)
	}
	if m["key"] != "value" {
		t.Errorf("m[key] = %v, want \"value\"", m["key"])
	}
	if m["num"] != int64(42) {
		t.Errorf("m[num] = %v, want 42", m["num"])
	}
}

func TestSortedKeys(t *testing.T) {
	tests := []struct {
		name string
		m    map[string]int
		want []string
	}{
		{"empty", map[string]int{}, nil},
		{"single", map[string]int{"a": 1}, []string{"a"}},
		{"multiple", map[string]int{"c": 3, "a": 1, "b": 2}, []string{"a", "b", "c"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sortedKeys(tt.m)
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("sortedKeys() returned %d keys, want %d", len(got), len(tt.want))
			}
			for i, k := range got {
				if k != tt.want[i] {
					t.Errorf("sortedKeys()[%d] = %q, want %q", i, k, tt.want[i])
				}
			}
		})
	}
}

func TestWriteCollectionCSV_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	docs := []docRecord{
		{path: "users/doc1", data: map[string]any{"name": "Alice", "age": int64(30)}},
		{path: "users/doc2", data: map[string]any{"name": "Bob", "age": int64(25)}},
	}
	fieldSet := map[string]struct{}{"name": {}, "age": {}}

	filePath, err := writeCollectionCSV(docs, fieldSet, "users", tmpDir, false)
	if err != nil {
		t.Fatalf("writeCollectionCSV() error = %v", err)
	}

	expected := filepath.Join(tmpDir, "users.csv")
	if filePath != expected {
		t.Errorf("filePath = %q, want %q", filePath, expected)
	}

	records := readCSV(t, filePath)
	if len(records) != 3 { // header + 2 rows
		t.Fatalf("expected 3 rows (header + 2), got %d", len(records))
	}

	// Headers: __path__, age, name (sorted)
	wantHeaders := []string{"__path__", "age", "name"}
	for i, h := range wantHeaders {
		if records[0][i] != h {
			t.Errorf("header[%d] = %q, want %q", i, records[0][i], h)
		}
	}

	// Row 1: users/doc1, 30, Alice
	if records[1][0] != "users/doc1" || records[1][1] != "30" || records[1][2] != "Alice" {
		t.Errorf("row 1 = %v, want [users/doc1 30 Alice]", records[1])
	}
}

func TestWriteCollectionCSV_EmptyDocs(t *testing.T) {
	tmpDir := t.TempDir()
	fieldSet := map[string]struct{}{"a": {}}

	filePath, err := writeCollectionCSV(nil, fieldSet, "empty", tmpDir, false)
	if err != nil {
		t.Fatalf("writeCollectionCSV() error = %v", err)
	}

	records := readCSV(t, filePath)
	if len(records) != 1 { // header only
		t.Fatalf("expected 1 row (header only), got %d", len(records))
	}
}

func TestWriteCollectionCSV_NestedPath(t *testing.T) {
	tmpDir := t.TempDir()
	docs := []docRecord{
		{path: "users/alice/orders/order1", data: map[string]any{"total": float64(99.99)}},
	}
	fieldSet := map[string]struct{}{"total": {}}

	filePath, err := writeCollectionCSV(docs, fieldSet, "users/orders", tmpDir, false)
	if err != nil {
		t.Fatalf("writeCollectionCSV() error = %v", err)
	}

	expected := filepath.Join(tmpDir, "users", "orders.csv")
	if filePath != expected {
		t.Errorf("filePath = %q, want %q", filePath, expected)
	}

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Error("expected file to exist at nested path")
	}
}

func TestWriteCollectionCSV_MissingFields(t *testing.T) {
	tmpDir := t.TempDir()
	docs := []docRecord{
		{path: "sparse/doc1", data: map[string]any{"a": "val_a"}},
		{path: "sparse/doc2", data: map[string]any{"b": "val_b"}},
	}
	fieldSet := map[string]struct{}{"a": {}, "b": {}}

	filePath, err := writeCollectionCSV(docs, fieldSet, "sparse", tmpDir, false)
	if err != nil {
		t.Fatalf("writeCollectionCSV() error = %v", err)
	}

	records := readCSV(t, filePath)
	// doc1 has "a" but not "b"
	if records[1][1] != "val_a" {
		t.Errorf("doc1.a = %q, want %q", records[1][1], "val_a")
	}
	if records[1][2] != "" {
		t.Errorf("doc1.b = %q, want empty", records[1][2])
	}
	// doc2 has "b" but not "a"
	if records[2][1] != "" {
		t.Errorf("doc2.a = %q, want empty", records[2][1])
	}
	if records[2][2] != "val_b" {
		t.Errorf("doc2.b = %q, want %q", records[2][2], "val_b")
	}
}

func TestWriteCollectionCSV_SpecialCharacters(t *testing.T) {
	tmpDir := t.TempDir()
	docs := []docRecord{
		{path: "special/doc1", data: map[string]any{"text": "hello, \"world\""}},
		{path: "special/doc2", data: map[string]any{"text": "line1\nline2"}},
	}
	fieldSet := map[string]struct{}{"text": {}}

	filePath, err := writeCollectionCSV(docs, fieldSet, "special", tmpDir, false)
	if err != nil {
		t.Fatalf("writeCollectionCSV() error = %v", err)
	}

	records := readCSV(t, filePath)
	if records[1][1] != `hello, "world"` {
		t.Errorf("row 1 text = %q, want %q", records[1][1], `hello, "world"`)
	}
	if records[2][1] != "line1\nline2" {
		t.Errorf("row 2 text = %q, want %q", records[2][1], "line1\nline2")
	}
}

func TestTypeLabel(t *testing.T) {
	fixedTime := time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC)

	tests := []struct {
		name  string
		input any
		want  string
	}{
		{"string", "hello", "string"},
		{"bool", true, "bool"},
		{"int64", int64(42), "int"},
		{"float64", float64(3.14), "float"},
		{"timestamp", fixedTime, "timestamp"},
		{"geo", &latlng.LatLng{Latitude: 1.0, Longitude: 2.0}, "geo"},
		{"bytes", []byte("abc"), "bytes"},
		{"array", []any{"a", "b"}, "array"},
		{"map", map[string]any{"k": "v"}, "map"},
		{"unknown", struct{}{}, "string"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := typeLabel(tt.input)
			if got != tt.want {
				t.Errorf("typeLabel(%T) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestTypeLabel_DocumentRef(t *testing.T) {
	client, err := firestore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Skipf("cannot create Firestore client: %v", err)
	}
	defer client.Close()

	ref := client.Doc("users/alice")
	if got := typeLabel(ref); got != "ref" {
		t.Errorf("typeLabel(DocumentRef) = %q, want %q", got, "ref")
	}
}

func TestWriteCollectionCSV_WithTypes(t *testing.T) {
	tmpDir := t.TempDir()
	fixedTime := time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC)
	docs := []docRecord{
		{path: "things/doc1", data: map[string]any{
			"name":   "Alice",
			"age":    int64(30),
			"active": true,
			"score":  float64(9.5),
			"joined": fixedTime,
		}},
		{path: "things/doc2", data: map[string]any{
			"name": "Bob",
			"age":  int64(25),
			"tags": []any{"a", "b"},
		}},
	}
	fieldSet := map[string]struct{}{
		"name": {}, "age": {}, "active": {}, "score": {}, "joined": {}, "tags": {},
	}

	filePath, err := writeCollectionCSV(docs, fieldSet, "things", tmpDir, true)
	if err != nil {
		t.Fatalf("writeCollectionCSV() error = %v", err)
	}

	records := readCSV(t, filePath)
	if len(records) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(records))
	}

	// Last header should be __fs_types__
	headers := records[0]
	if headers[len(headers)-1] != "__fs_types__" {
		t.Errorf("last header = %q, want __fs_types__", headers[len(headers)-1])
	}

	// Parse __fs_types__ for row 1 (doc1 has name, age, active, score, joined)
	var types1 map[string]string
	if err := json.Unmarshal([]byte(records[1][len(headers)-1]), &types1); err != nil {
		t.Fatalf("failed to parse __fs_types__ for row 1: %v", err)
	}
	wantTypes1 := map[string]string{
		"name": "string", "age": "int", "active": "bool",
		"score": "float", "joined": "timestamp",
	}
	for k, v := range wantTypes1 {
		if types1[k] != v {
			t.Errorf("row 1 type[%s] = %q, want %q", k, types1[k], v)
		}
	}

	// Parse __fs_types__ for row 2 (doc2 has name, age, tags)
	var types2 map[string]string
	if err := json.Unmarshal([]byte(records[2][len(headers)-1]), &types2); err != nil {
		t.Fatalf("failed to parse __fs_types__ for row 2: %v", err)
	}
	if types2["tags"] != "array" {
		t.Errorf("row 2 type[tags] = %q, want %q", types2["tags"], "array")
	}
	// Fields not present in doc2 should not appear in its type map
	if _, ok := types2["active"]; ok {
		t.Error("row 2 should not have type for 'active' (field not present)")
	}
}

func TestWriteCollectionCSV_WithoutTypes(t *testing.T) {
	tmpDir := t.TempDir()
	docs := []docRecord{
		{path: "col/doc1", data: map[string]any{"name": "Alice"}},
	}
	fieldSet := map[string]struct{}{"name": {}}

	filePath, err := writeCollectionCSV(docs, fieldSet, "col", tmpDir, false)
	if err != nil {
		t.Fatalf("writeCollectionCSV() error = %v", err)
	}

	records := readCSV(t, filePath)
	headers := records[0]
	for _, h := range headers {
		if h == "__fs_types__" {
			t.Error("__fs_types__ should not be present when withTypes=false")
		}
	}
}

// readCSV is a test helper that reads all records from a CSV file.
func readCSV(t *testing.T, path string) [][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open %s: %v", path, err)
	}
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("failed to read CSV %s: %v", path, err)
	}
	return records
}

// newTestCommand creates a root command with subcommands for testing CLI parsing.
func newTestCommand() *cobra.Command {
	root := &cobra.Command{
		Use:           "firestore2csv",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	pf := root.PersistentFlags()
	pf.StringP("project", "p", "", "GCP project ID")
	pf.StringP("emulator", "e", "", "Firestore emulator host")
	pf.StringP("database", "d", "(default)", "Firestore database name")

	exportCmd := &cobra.Command{
		Use:          "export",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			_, _, _, err := validateConnectionFlags(cmd)
			return err
		},
	}
	ef := exportCmd.Flags()
	ef.StringP("collections", "c", "", "")
	ef.IntP("limit", "l", 0, "")
	ef.Int("child-limit", 0, "")
	ef.Int("depth", -1, "")
	ef.StringP("output", "o", ".", "")

	importCmd := &cobra.Command{
		Use:          "import",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			_, _, _, err := validateConnectionFlags(cmd)
			return err
		},
	}
	imf := importCmd.Flags()
	imf.StringSliceP("input", "i", []string{"."}, "")
	imf.String("on-conflict", "skip", "")
	imf.Bool("dry-run", false, "")

	root.AddCommand(exportCmd)
	root.AddCommand(importCmd)
	return root
}

func TestValidateConnectionFlags(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "no project or emulator",
			args:    []string{"export"},
			wantErr: "exactly one of --project or --emulator must be provided",
		},
		{
			name:    "both project and emulator",
			args:    []string{"export", "-p", "my-project", "-e", "localhost:8686"},
			wantErr: "--project and --emulator are mutually exclusive",
		},
		{
			name: "project only",
			args: []string{"export", "-p", "my-project"},
		},
		{
			name: "emulator only",
			args: []string{"export", "-e", "localhost:8686"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newTestCommand()
			cmd.SetArgs(tt.args)
			err := cmd.Execute()
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if err.Error() != tt.wantErr {
					t.Errorf("error = %q, want %q", err.Error(), tt.wantErr)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestCastValue(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		typeName string
		want     any
		wantErr  bool
	}{
		{"empty string", "", "string", nil, false},
		{"empty int", "", "int", nil, false},
		{"string", "hello", "string", "hello", false},
		{"bool true", "true", "bool", true, false},
		{"bool false", "false", "bool", false, false},
		{"bool invalid", "yes", "bool", nil, true},
		{"int", "42", "int", int64(42), false},
		{"int negative", "-100", "int", int64(-100), false},
		{"int invalid", "abc", "int", nil, true},
		{"float", "3.14", "float", float64(3.14), false},
		{"float invalid", "abc", "float", nil, true},
		{"timestamp", "2024-06-15T12:00:00Z", "timestamp", time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC), false},
		{"timestamp invalid", "not-a-time", "timestamp", nil, true},
		{"geo", `{"lat":37.7749,"lng":-122.4194}`, "geo", &latlng.LatLng{Latitude: 37.7749, Longitude: -122.4194}, false},
		{"geo missing lat", `{"lng":1.0}`, "geo", nil, true},
		{"geo invalid json", `not json`, "geo", nil, true},
		{"bytes", base64.StdEncoding.EncodeToString([]byte("hello")), "bytes", []byte("hello"), false},
		{"bytes invalid", "!!!not-base64", "bytes", nil, true},
		{"ref", "users/alice", "ref", "users/alice", false},
		{"array", `["a","b"]`, "array", []any{"a", "b"}, false},
		{"array invalid", `not json`, "array", nil, true},
		{"map", `{"key":"val"}`, "map", map[string]any{"key": "val"}, false},
		{"map invalid", `not json`, "map", nil, true},
		{"unknown type", "x", "unknown", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := castValue(tt.raw, tt.typeName)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			switch w := tt.want.(type) {
			case nil:
				if got != nil {
					t.Errorf("got %v (%T), want nil", got, got)
				}
			case []byte:
				g, ok := got.([]byte)
				if !ok {
					t.Fatalf("got %T, want []byte", got)
				}
				if string(g) != string(w) {
					t.Errorf("got %v, want %v", g, w)
				}
			case *latlng.LatLng:
				g, ok := got.(*latlng.LatLng)
				if !ok {
					t.Fatalf("got %T, want *latlng.LatLng", got)
				}
				if g.Latitude != w.Latitude || g.Longitude != w.Longitude {
					t.Errorf("got {%v,%v}, want {%v,%v}", g.Latitude, g.Longitude, w.Latitude, w.Longitude)
				}
			case []any:
				g, ok := got.([]any)
				if !ok {
					t.Fatalf("got %T, want []any", got)
				}
				if len(g) != len(w) {
					t.Errorf("got len %d, want len %d", len(g), len(w))
				}
			case map[string]any:
				g, ok := got.(map[string]any)
				if !ok {
					t.Fatalf("got %T, want map[string]any", got)
				}
				if len(g) != len(w) {
					t.Errorf("got len %d, want len %d", len(g), len(w))
				}
			default:
				if got != tt.want {
					t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
				}
			}
		})
	}
}

func TestDetectType(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want any
	}{
		{"empty", "", nil},
		{"true", "true", true},
		{"false", "false", false},
		{"True is string", "True", "True"},
		{"timestamp", "2024-06-15T12:00:00Z", time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)},
		{"integer", "42", int64(42)},
		{"negative int", "-100", int64(-100)},
		{"float", "42.0", float64(42.0)},
		{"float scientific", "1.5e2", float64(150.0)},
		{"json object", `{"key":"val"}`, map[string]any{"key": "val"}},
		{"json array", `["a","b"]`, []any{"a", "b"}},
		{"plain string", "hello world", "hello world"},
		{"number-like string", "123abc", "123abc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := detectType(tt.raw)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			switch w := tt.want.(type) {
			case nil:
				if got != nil {
					t.Errorf("got %v (%T), want nil", got, got)
				}
			case time.Time:
				g, ok := got.(time.Time)
				if !ok {
					t.Fatalf("got %T, want time.Time", got)
				}
				if !g.Equal(w) {
					t.Errorf("got %v, want %v", g, w)
				}
			case map[string]any:
				g, ok := got.(map[string]any)
				if !ok {
					t.Fatalf("got %T, want map[string]any", got)
				}
				if len(g) != len(w) {
					t.Errorf("got len %d, want len %d", len(g), len(w))
				}
			case []any:
				g, ok := got.([]any)
				if !ok {
					t.Fatalf("got %T, want []any", got)
				}
				if len(g) != len(w) {
					t.Errorf("got len %d, want len %d", len(g), len(w))
				}
			default:
				if got != tt.want {
					t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
				}
			}
		})
	}
}

func TestParseCSVFile_WithTypes(t *testing.T) {
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	content := `__path__,name,age,active,__fs_types__
users/alice,Alice,30,true,"{""name"":""string"",""age"":""int"",""active"":""bool""}"
users/bob,Bob,25,false,"{""name"":""string"",""age"":""int"",""active"":""bool""}"
`
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	records, err := parseCSVFile(csvPath)
	if err != nil {
		t.Fatalf("parseCSVFile() error = %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	r := records[0]
	if r.path != "users/alice" {
		t.Errorf("path = %q, want %q", r.path, "users/alice")
	}
	if r.data["name"] != "Alice" {
		t.Errorf("name = %v, want Alice", r.data["name"])
	}
	if r.data["age"] != int64(30) {
		t.Errorf("age = %v (%T), want int64(30)", r.data["age"], r.data["age"])
	}
	if r.data["active"] != true {
		t.Errorf("active = %v, want true", r.data["active"])
	}
}

func TestParseCSVFile_WithoutTypes(t *testing.T) {
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	content := `__path__,name,age,active
users/alice,Alice,30,true
users/bob,Bob,25,false
`
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	records, err := parseCSVFile(csvPath)
	if err != nil {
		t.Fatalf("parseCSVFile() error = %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	r := records[0]
	// Without types, heuristics apply: 30 → int64, true → bool
	if r.data["age"] != int64(30) {
		t.Errorf("age = %v (%T), want int64(30)", r.data["age"], r.data["age"])
	}
	if r.data["active"] != true {
		t.Errorf("active = %v, want true", r.data["active"])
	}
	if r.data["name"] != "Alice" {
		t.Errorf("name = %v, want Alice", r.data["name"])
	}
}

func TestParseCSVFile_MissingPath(t *testing.T) {
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	content := "name,age\nAlice,30\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	_, err := parseCSVFile(csvPath)
	if err == nil {
		t.Fatal("expected error for missing __path__ column")
	}
}

func TestParseCSVFile_EmptyValues(t *testing.T) {
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	content := `__path__,name,age
users/alice,Alice,
users/bob,,25
`
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	records, err := parseCSVFile(csvPath)
	if err != nil {
		t.Fatalf("parseCSVFile() error = %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	// alice: age is empty → nil → omitted from data
	if _, ok := records[0].data["age"]; ok {
		t.Error("alice should not have 'age' key (empty value)")
	}
	// bob: name is empty → nil → omitted from data
	if _, ok := records[1].data["name"]; ok {
		t.Error("bob should not have 'name' key (empty value)")
	}
}

func TestSubcommandStructure(t *testing.T) {
	t.Run("root without subcommand prints help", func(t *testing.T) {
		cmd := newTestCommand()
		var buf bytes.Buffer
		cmd.SetOut(&buf)
		cmd.SetArgs([]string{})
		_ = cmd.Execute()
		// Root should not error — it just prints help
	})

	t.Run("export subcommand is recognized", func(t *testing.T) {
		cmd := newTestCommand()
		cmd.SetArgs([]string{"export", "-p", "test"})
		if err := cmd.Execute(); err != nil {
			t.Errorf("export subcommand error: %v", err)
		}
	})

	t.Run("import subcommand is recognized", func(t *testing.T) {
		cmd := newTestCommand()
		cmd.SetArgs([]string{"import", "-e", "localhost:8686"})
		if err := cmd.Execute(); err != nil {
			t.Errorf("import subcommand error: %v", err)
		}
	})
}
