# Implementation Plan: Data Sanitization

## Prerequisites

Add the `gofakeit` dependency:

```bash
go get github.com/brianvokes/gofakeit/v7
```

## 1. Config Parsing {#config-parsing}

**File:** `sanitize.go`

Create a `sanitizeConfig` struct with a `Fields` map (`map[string]string` — field name → faker type).

Implement a `parseSanitizeConfig(raw string) (sanitizeConfig, error)` function:

- If `raw` ends with `.yaml` or `.yml`, read the file and unmarshal the YAML `fields` map. This requires a YAML library — use `gopkg.in/yaml.v3` (already idiomatic for Go CLIs).
- Otherwise, parse as comma-separated `key=type` pairs (e.g., `"email=email,name=firstName"`).
- Validate every faker type against a known set of supported types. Return a clear error for unknown types, listing what's supported.

The set of valid faker types: `firstName`, `lastName`, `email`, `phone`, `address`, `companyName`, `uuid`.

## 2. Sanitizer Engine {#sanitizer-engine}

**File:** `sanitize.go`

Create a `sanitizer` struct:

```
sanitizer {
    fields map[string]string    // field name → faker type
    faker  *gofakeit.Faker      // seeded or random instance
}
```

Constructor: `newSanitizer(cfg sanitizeConfig, seed int64) *sanitizer`

- If `seed == 0`, use `gofakeit.New(0)` (random source).
- If `seed != 0`, use `gofakeit.NewFaker(source, false)` with `source` seeded from the given seed, giving deterministic output.

**Note:** Check the gofakeit v7 API for the exact constructor — `gofakeit.New(seed)` may already handle this where `seed=0` is random. Confirm in docs.

Core method: `sanitizeRecord(data map[string]any)` — mutates the map in place:

- Iterate over all keys in the map.
- If a key matches the `fields` config and the value is a `string`, replace it with the output of the appropriate gofakeit function.
- If the value is a `map[string]any`, recurse into it.
- If the value is a `[]any`, iterate elements; for each element that is a `map[string]any`, recurse.

Implement a `generate(fakerType string) string` method that dispatches to the right gofakeit function based on the type string. Use a switch statement.

## 3. Unit Tests for Sanitizer {#sanitizer-tests}

**File:** `sanitize_test.go`

Test the following:

### Config parsing tests
- Parse inline format: `"email=email,name=firstName"` → correct map.
- Parse YAML file: create a temp YAML file, parse it, verify the map.
- Reject unknown faker types with a descriptive error.
- Reject malformed inline strings (missing `=`, empty key, etc.).

### Sanitizer tests
- Flat map: fields matching the config are replaced, non-matching fields are untouched.
- Nested map: fields inside nested `map[string]any` are sanitized.
- Array of maps: fields inside maps within `[]any` are sanitized.
- Non-string values: matched field names with non-string values (int, bool) are left as-is.
- Seeded determinism: two sanitizers with the same seed produce the same output for the same input.
- Seed=0 randomness: two sanitizers with seed=0 produce different output (probabilistic — use a large enough sample).

## 4. Export Integration {#export-integration}

**File:** `main.go`

### CLI flags
- Add `--sanitize` flag (string, default empty) to the `export` subcommand.
- Add `--seed` flag (int64, default 0) to the `export` subcommand.

### Wiring
In the `run()` function (export command handler):
- If `--sanitize` is non-empty, parse the config via `parseSanitizeConfig()`, construct a `sanitizer` via `newSanitizer()`.
- Pass the sanitizer through the export pipeline. The cleanest integration point is in `readAndExportCollection` and `readAndExportAggregated`: after documents are collected into `[]docRecord` but before `writeCollectionCSV` is called, iterate the docs and call `sanitizer.sanitizeRecord(doc.data)` on each.
- This requires threading the sanitizer (or nil) through `exportConfig` → `runExport` → `exportCollectionTree` → `readAndExportCollection` / `readAndExportAggregated`. Add a `sanitizer *sanitizer` field to `exportConfig`.
- When `sanitizer` is nil, no transformation happens (zero-cost path for non-sanitized exports).

## 5. Sanitize Subcommand {#sanitize-subcommand}

**File:** `main.go` (command registration), `sanitize.go` (logic)

### CLI registration
In `main()`, add a `sanitize` subcommand with:
- `--config` (string, required) — sanitization config (inline or YAML path, same polymorphic parsing).
- `--input` / `-i` (string, default `.`) — input file or directory.
- `--output` / `-o` (string, required) — output directory.
- `--seed` (int64, default 0) — random seed.

### Logic (`runSanitize` function in `sanitize.go`)

1. Parse config via `parseSanitizeConfig()`.
2. Construct sanitizer via `newSanitizer()`.
3. Discover CSV files in the input path. Reuse or mirror the `discoverCSVFiles()` function from import logic.
4. For each CSV file:
   a. Read the CSV (headers + rows).
   b. Identify which column indices correspond to fields in the sanitization config. Skip special columns (`__path__`, `__fs_types__`).
   c. For each row, replace values in matched columns using `sanitizer.generate(fakerType)`.
   d. Write the sanitized CSV to the output directory, preserving relative path structure from the input root.
5. Print a summary (files processed, rows sanitized).

**Important difference from export integration:** The sanitize subcommand works on flat CSV string values directly (no `map[string]any` — just column-name-to-cell matching). The `generate()` method is called directly, not `sanitizeRecord()`. Nested structures in CSV are JSON strings — sanitizing individual fields within a JSON blob is out of scope for the subcommand (it operates at the column level only).

## 6. Integration Tests {#integration-tests}

**File:** `integration_test.go` (add to existing integration test file, gated with `//go:build integration`)

### Export + sanitize test
- Seed test data into the emulator with known PII values.
- Run export with `--sanitize` and a known `--seed`.
- Read the output CSV and verify:
  - Configured fields contain fake data (not the originals).
  - Non-configured fields are untouched.
  - With the same seed, a second export produces identical output.

### Sanitize subcommand test
- Create a CSV file with known content.
- Run the sanitize logic with a config and seed.
- Verify output CSV has replaced values in configured columns, preserved everything else.
- Verify directory structure is mirrored in output.

## 7. Summary of files touched

| File                   | Changes                                                      |
|-----------------------|--------------------------------------------------------------|
| `go.mod` / `go.sum`  | Add `gofakeit` and `yaml.v3` dependencies                   |
| `sanitize.go` (new)  | Config parsing, sanitizer struct, generate, sanitizeRecord, runSanitize |
| `sanitize_test.go` (new) | Unit tests for config parsing and sanitizer logic        |
| `main.go`            | Add `--sanitize`/`--seed` flags to export, register `sanitize` subcommand, thread sanitizer through export pipeline |
| `integration_test.go`| Integration tests for export+sanitize and sanitize subcommand |
