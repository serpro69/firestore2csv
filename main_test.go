package main

import (
	"encoding/base64"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/type/latlng"
)

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
		{id: "doc1", data: map[string]any{"name": "Alice", "age": int64(30)}},
		{id: "doc2", data: map[string]any{"name": "Bob", "age": int64(25)}},
	}
	fieldSet := map[string]struct{}{"name": {}, "age": {}}

	filePath, err := writeCollectionCSV(docs, fieldSet, "users", tmpDir)
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

	// Headers: __document_id__, age, name (sorted)
	wantHeaders := []string{"__document_id__", "age", "name"}
	for i, h := range wantHeaders {
		if records[0][i] != h {
			t.Errorf("header[%d] = %q, want %q", i, records[0][i], h)
		}
	}

	// Row 1: doc1, 30, Alice
	if records[1][0] != "doc1" || records[1][1] != "30" || records[1][2] != "Alice" {
		t.Errorf("row 1 = %v, want [doc1 30 Alice]", records[1])
	}
}

func TestWriteCollectionCSV_EmptyDocs(t *testing.T) {
	tmpDir := t.TempDir()
	fieldSet := map[string]struct{}{"a": {}}

	filePath, err := writeCollectionCSV(nil, fieldSet, "empty", tmpDir)
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
		{id: "order1", data: map[string]any{"total": float64(99.99)}},
	}
	fieldSet := map[string]struct{}{"total": {}}

	filePath, err := writeCollectionCSV(docs, fieldSet, "users/orders", tmpDir)
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
		{id: "doc1", data: map[string]any{"a": "val_a"}},
		{id: "doc2", data: map[string]any{"b": "val_b"}},
	}
	fieldSet := map[string]struct{}{"a": {}, "b": {}}

	filePath, err := writeCollectionCSV(docs, fieldSet, "sparse", tmpDir)
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
		{id: "doc1", data: map[string]any{"text": "hello, \"world\""}},
		{id: "doc2", data: map[string]any{"text": "line1\nline2"}},
	}
	fieldSet := map[string]struct{}{"text": {}}

	filePath, err := writeCollectionCSV(docs, fieldSet, "special", tmpDir)
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
