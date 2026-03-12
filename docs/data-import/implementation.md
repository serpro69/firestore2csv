# Import Feature — Implementation Plan

## Prerequisites

- Familiarity with Go, Cobra CLI framework, and the `cloud.google.com/go/firestore` package.
- Read `design.md` in this directory for the full feature specification.
- The codebase is a single-file Go CLI (`main.go`) — all code lives there.
- Tests: unit tests in `main_test.go`, integration tests in `integration_test.go` (gated with `//go:build integration`, run against a local Firestore emulator).

## Phase 1: CLI Structure Refactor

### 1.1 Introduce Subcommands

**File:** `main.go`

The current `main()` function creates a single root `cobra.Command` with `RunE: run`. This must be refactored into:

- A root command with no `RunE` (prints help by default).
- An `export` subcommand that carries all current flags and behavior.
- An `import` subcommand (initially a stub).

The `run()` function becomes the `RunE` for the `export` subcommand. All current flags (`--project`, `--collections`, `--limit`, `--child-limit`, `--depth`, `--output`) move to the `export` subcommand.

### 1.2 Shared Connection Flags

**File:** `main.go`

Both subcommands need `--project`, `--emulator`, and `--database`. These should be defined as persistent flags on the root command, or as a shared helper that registers them on each subcommand.

Add validation: exactly one of `--project` or `--emulator` must be provided. When `--emulator` is set, the tool should call `os.Setenv("FIRESTORE_EMULATOR_HOST", value)` before creating the Firestore client, and use a placeholder project ID (e.g. `"emulator-project"`).

The existing Firestore client creation in `runExport()` should be extracted into a shared helper function (e.g. `newFirestoreClient(ctx, project, database, emulator)`) used by both export and import.

### 1.3 Update Tests

**Files:** `main_test.go`, `integration_test.go`

- Unit tests that call `run()` or `runExport()` directly should still work — the function signatures don't change, just how Cobra invokes them.
- Integration tests that use `runExport()` should be verified to still pass.
- Add a basic test that the CLI parses subcommands correctly (export and import are recognized, help text is correct).

## Phase 2: Export Format Changes

### 2.1 Replace `__document_id__` with `__path__`

**File:** `main.go`

Modify `writeCollectionCSV()` and the `docRecord` struct:

- The `docRecord` struct currently has an `id` field. Change it to `path` (or add a `path` field) that stores the full Firestore document path (e.g. `users/alice`).
- In `readAndExportCollection()` and `readAndExportAggregated()`, where `docRecord` is created, use `snap.Ref.Path` to get the full path. Note: `snap.Ref.Path` returns the full resource path including project/database prefix — you need just the document path portion (collection/doc/collection/doc). Extract this from `snap.Ref` by walking the parent chain or by trimming the prefix. Check what `snap.Ref.Path` actually returns versus what you need.
- In `writeCollectionCSV()`, change the header from `__document_id__` to `__path__` and write `doc.path` instead of `doc.id`.

### 2.2 Add `--with-types` Flag and `__fs_types__` Column

**File:** `main.go`

- Add `--with-types` boolean flag to the `export` subcommand.
- Pass it through `exportConfig` to `writeCollectionCSV()`.
- When enabled, add `__fs_types__` as the last header column.
- For each row, build a JSON object mapping every field name to its type label. Create a `typeLabel(v any) string` function that returns `"string"`, `"bool"`, `"int"`, `"float"`, `"timestamp"`, `"geo"`, `"bytes"`, `"ref"`, `"array"`, or `"map"` based on the Go type of the value (similar structure to `formatValue()`).
- Write the minified JSON as the last cell in each row.

### 2.3 Update Export Tests

**Files:** `main_test.go`, `integration_test.go`

- Update all tests that check CSV output to expect `__path__` instead of `__document_id__`.
- Add unit tests for `typeLabel()`.
- Add unit tests for `writeCollectionCSV()` with `--with-types` enabled, verifying the `__fs_types__` column content.
- Update integration tests to verify `__path__` contains correct full document paths, including sub-collection paths.

## Phase 3: Import Implementation

### 3.1 CSV Parsing and Type Reconstruction

**File:** `main.go`

Create the core import parsing functions:

- `parseCSVFile(path string) ([]importRecord, error)` — reads a CSV file, returns parsed records. Each record contains the document path (from `__path__`) and a `map[string]any` of field values cast to their correct Go/Firestore types.
- `castValue(raw string, typeName string) (any, error)` — converts a CSV string to the correct Go type based on the type label. Handle all type labels from the design doc. For `timestamp`, parse RFC3339Nano. For `geo`, parse `{"lat":...,"lng":...}` JSON into `*latlng.LatLng`. For `bytes`, decode base64. For `ref`, the value is a path string — store it as-is (or reconstruct a `DocumentRef` during the write phase). For `array`/`map`, parse JSON.
- `detectType(raw string) (any, error)` — heuristic type detection for when `__fs_types__` is absent. Follow the priority order defined in the design doc.

### 3.2 Import Orchestration

**File:** `main.go`

Create the import runner:

- `importConfig` struct — mirrors `exportConfig` with fields for: `project`, `database`, `emulator`, `inputs` ([]string), `onConflict` (string), `dryRun` (bool).
- `runImport(cfg importConfig) error` — main import entry point:
  1. Resolve all input paths: for each `--input` value, if directory → recursively glob `**/*.csv`, if file → use directly. Collect all CSV file paths.
  2. Create Firestore client (using the shared helper from Phase 1).
  3. For each CSV file, call `parseCSVFile()` to get import records.
  4. Group records by collection path (derived from `__path__`).
  5. For each record, apply the conflict strategy.
  6. Write documents to Firestore (or report in dry-run mode).
  7. Print progress and summary.

### 3.3 Conflict Handling

**File:** `main.go`

Implement each conflict strategy:

- **`skip`**: Before writing, call `doc.Get()` to check existence. If it exists, log a notice (document path + "skipped, already exists") and continue. Count skipped docs for the summary.
- **`overwrite`**: Use `client.Doc(path).Set(ctx, data)` — no existence check needed.
- **`merge`**: Use `client.Doc(path).Set(ctx, data, firestore.MergeAll)`.
- **`fail`**: First pass: check existence of all documents across all CSVs. If any exist, print the conflicting paths and abort before writing anything. Second pass: write all documents.

### 3.4 Dry-Run Mode

**File:** `main.go`

When `--dry-run` is set, run the full pipeline (CSV parsing, type casting, conflict detection) but replace Firestore writes with no-ops. Print:
- Each collection that would be written to, with document count.
- Any conflicts detected (with paths and the action that would be taken).
- Any type casting warnings or errors.
- A final summary line.

### 3.5 Import Command Wiring

**File:** `main.go`

Wire the `import` subcommand in `main()`:

- Register flags: `--input` (string slice, default `["."]`), `--on-conflict` (string, default `"skip"`), `--dry-run` (bool, default `false`).
- Validate `--on-conflict` value is one of: `overwrite`, `merge`, `skip`, `fail`.
- `RunE` function reads flags, builds `importConfig`, calls `runImport()`.

### 3.6 Import Tests

**Files:** `main_test.go`, `integration_test.go`

Unit tests:
- `castValue()` — test all type labels with valid and invalid inputs.
- `detectType()` — test heuristic detection for each type, edge cases (e.g. `"42"` → int, `"42.0"` → float, `"true"` → bool, RFC3339 string → timestamp, JSON → map/array, plain string → string).
- `parseCSVFile()` — test with and without `__fs_types__` column, mixed types, missing values.

Integration tests:
- Round-trip test: export a seeded Firestore collection (with `--with-types`), import the CSV into a fresh collection, read back and compare.
- Conflict strategies: seed a collection, import with each `--on-conflict` mode, verify behavior.
- Sub-collection round-trip: export collections with sub-collections, import, verify hierarchy.
- Dry-run: verify no documents are written.
- Emulator flag: verify `--emulator` works correctly (integration tests already use the emulator).

## Phase 4: Documentation and Cleanup

### 4.1 Update CLAUDE.md

Reflect the new subcommand structure and import functionality.

### 4.2 Update README (if exists)

Add import usage examples and flag documentation.
