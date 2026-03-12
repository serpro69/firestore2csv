# Tasks: Firestore Import Feature

> Design: [./design.md](./design.md)
> Implementation: [./implementation.md](./implementation.md)
> Status: pending
> Created: 2026-03-12

## Task 1: CLI structure refactor — subcommands and shared connection
- **Status:** done
- **Depends on:** —
- **Docs:** [implementation.md — Phase 1](./implementation.md#phase-1-cli-structure-refactor)

### Subtasks
- [x] 1.1 Refactor `main()` in `main.go`: replace the single root `cobra.Command` with a root command (no `RunE`, prints help) and an `export` subcommand that carries all current flags and the existing `run` function as its `RunE`
- [x] 1.2 Add `--emulator` / `-e` flag and `--project` / `-p` flag as shared flags on both subcommands. Add validation: exactly one of `--project` or `--emulator` must be provided
- [x] 1.3 Extract Firestore client creation from `runExport()` into a shared `newFirestoreClient(ctx, project, database, emulator string) (*firestore.Client, error)` function. When `emulator` is set, call `os.Setenv("FIRESTORE_EMULATOR_HOST", emulator)` and use a placeholder project ID
- [x] 1.4 Add a stub `import` subcommand with its flags (`--input`, `--on-conflict`, `--dry-run`) and a placeholder `RunE` that returns `"not yet implemented"`
- [x] 1.5 Update existing unit tests in `main_test.go` and integration tests in `integration_test.go` to work with the refactored structure. Verify all existing tests pass

## Task 2: Export format — replace `__document_id__` with `__path__`
- **Status:** pending
- **Depends on:** Task 1
- **Docs:** [implementation.md — Phase 2.1](./implementation.md#21-replace-__document_id__-with-__path__)

### Subtasks
- [ ] 2.1 Update the `docRecord` struct in `main.go`: replace the `id` field with a `path` field that stores the full Firestore document path (e.g. `users/alice`, `users/alice/orders/order1`)
- [ ] 2.2 In `readAndExportCollection()` and `readAndExportAggregated()`, populate `docRecord.path` using the document ref. Determine the correct way to extract the document path from `snap.Ref` (it may include project/database prefix that needs trimming)
- [ ] 2.3 In `writeCollectionCSV()`, change the first header from `__document_id__` to `__path__` and write `doc.path` instead of `doc.id`
- [ ] 2.4 Update all unit and integration tests that check CSV output to expect `__path__` instead of `__document_id__`, including verifying correct full paths for sub-collection documents

## Task 3: Export format — add `--with-types` and `__fs_types__` column
- **Status:** pending
- **Depends on:** Task 2
- **Docs:** [implementation.md — Phase 2.2](./implementation.md#22-add---with-types-flag-and-__fs_types__-column)

### Subtasks
- [ ] 3.1 Add a `typeLabel(v any) string` function in `main.go` that returns the type label (`"string"`, `"bool"`, `"int"`, `"float"`, `"timestamp"`, `"geo"`, `"bytes"`, `"ref"`, `"array"`, `"map"`) based on the Go type of the Firestore value — similar switch structure to `formatValue()`
- [ ] 3.2 Add `--with-types` boolean flag to the `export` subcommand. Pass it through `exportConfig` down to `writeCollectionCSV()`
- [ ] 3.3 In `writeCollectionCSV()`, when `withTypes` is true: append `__fs_types__` to headers, and for each row build a JSON object mapping every field name to `typeLabel(value)` and write it as the last cell
- [ ] 3.4 Write unit tests for `typeLabel()` covering all Firestore types
- [ ] 3.5 Write unit tests for `writeCollectionCSV()` with `withTypes=true`, verifying correct `__fs_types__` column content including rows with mixed types
- [ ] 3.6 Add integration test verifying `__fs_types__` contains correct types for a collection with diverse Firestore types

## Task 4: Import — CSV parsing and type reconstruction
- **Status:** pending
- **Depends on:** Task 3
- **Docs:** [implementation.md — Phase 3.1](./implementation.md#31-csv-parsing-and-type-reconstruction)

### Subtasks
- [ ] 4.1 Create `castValue(raw string, typeName string) (any, error)` in `main.go` — converts a CSV string to the correct Go type based on type label. Handle: `string` (passthrough), `bool` (parse), `int` (parse int64), `float` (parse float64), `timestamp` (parse RFC3339Nano → `time.Time`), `geo` (parse JSON → `*latlng.LatLng`), `bytes` (base64 decode), `ref` (store as string path), `array` (parse JSON → `[]any`), `map` (parse JSON → `map[string]any`)
- [ ] 4.2 Create `detectType(raw string) (any, error)` in `main.go` — heuristic detection following priority: empty → nil, `true`/`false` → bool, RFC3339 → timestamp, integer → int64, float → float64, JSON object → map, JSON array → array, else → string
- [ ] 4.3 Create `parseCSVFile(path string) ([]importRecord, error)` in `main.go` — reads a CSV file, extracts `__path__` and optionally `__fs_types__`, and for each row produces an `importRecord` with the document path and a `map[string]any` of typed field values. Use `castValue` when types are available, `detectType` otherwise
- [ ] 4.4 Write unit tests for `castValue()` — all type labels with valid inputs, error cases for malformed values
- [ ] 4.5 Write unit tests for `detectType()` — each heuristic case, edge cases like `"42"` → int, `"42.0"` → float, `"true"` → bool, ISO timestamp, JSON object/array, plain string
- [ ] 4.6 Write unit tests for `parseCSVFile()` — CSV with and without `__fs_types__`, mixed types across rows, missing values, empty `__fs_types__` cells

## Task 5: Import — orchestration, conflict handling, and dry-run
- **Status:** pending
- **Depends on:** Task 4
- **Docs:** [implementation.md — Phase 3.2 through 3.5](./implementation.md#32-import-orchestration)

### Subtasks
- [ ] 5.1 Create `importConfig` struct in `main.go` with fields: `project`, `database`, `emulator`, `inputs` ([]string), `onConflict` (string), `dryRun` (bool)
- [ ] 5.2 Create `discoverCSVFiles(inputs []string) ([]string, error)` — for each input, if directory → recursively glob `**/*.csv`, if file → use directly. Return deduplicated list of CSV paths
- [ ] 5.3 Create `runImport(cfg importConfig) error` — main orchestration: discover CSV files, create Firestore client, parse each CSV via `parseCSVFile()`, write documents with conflict handling, print progress and summary
- [ ] 5.4 Implement conflict strategies in the write loop: `skip` (check existence with `doc.Get()`, log notice if exists), `overwrite` (`doc.Set()`), `merge` (`doc.Set()` with `MergeAll`), `fail` (pre-check all documents, abort if any exist)
- [ ] 5.5 Implement dry-run mode: skip Firestore writes, print summary of what would happen (collections, document counts, conflicts, type warnings)
- [ ] 5.6 Wire the `import` subcommand's `RunE` in `main()`: read flags, validate `--on-conflict` value, build `importConfig`, call `runImport()`
- [ ] 5.7 Handle `ref` type during write: when a field has type `ref`, convert the path string to a `*firestore.DocumentRef` using the Firestore client before writing

## Task 6: Import — integration tests
- **Status:** pending
- **Depends on:** Task 5
- **Docs:** [implementation.md — Phase 3.6](./implementation.md#36-import-tests)

### Subtasks
- [ ] 6.1 Round-trip test in `integration_test.go`: seed Firestore via emulator, export with `--with-types`, import the CSV into a different collection, read back all documents and compare field values and types
- [ ] 6.2 Conflict strategy tests: seed a collection, attempt import with each `--on-conflict` mode (`skip`, `overwrite`, `merge`, `fail`), verify correct behavior per strategy
- [ ] 6.3 Sub-collection round-trip: seed collections with sub-collections, export, import into fresh collections, verify parent-child hierarchy is preserved
- [ ] 6.4 Dry-run test: run import with `--dry-run`, verify zero documents written to Firestore
- [ ] 6.5 Heuristic import test: import a CSV without `__fs_types__` column, verify types are correctly inferred via heuristics

## Task 7: Final verification
- **Status:** pending
- **Depends on:** Task 1, Task 2, Task 3, Task 4, Task 5, Task 6

### Subtasks
- [ ] 7.1 Run `testing-process` skill to verify all tasks — full test suite (unit + integration), edge cases
- [ ] 7.2 Run `documentation-process` skill to update CLAUDE.md and any other relevant docs
- [ ] 7.3 Run `solid-code-review go` skill to review the implementation
