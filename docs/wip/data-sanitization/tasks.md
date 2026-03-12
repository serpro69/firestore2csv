# Tasks: Data Sanitization

> Design: [./design.md](./design.md)
> Implementation: [./implementation.md](./implementation.md)
> Status: pending
> Created: 2026-03-12

## Task 1: Config parsing and sanitizer engine
- **Status:** pending
- **Depends on:** —
- **Docs:** [implementation.md#config-parsing](./implementation.md#config-parsing), [implementation.md#sanitizer-engine](./implementation.md#sanitizer-engine)

### Subtasks
- [ ] 1.1 Add `gofakeit/v7` and `gopkg.in/yaml.v3` dependencies via `go get`
- [ ] 1.2 Create `sanitize.go` with `sanitizeConfig` struct (`Fields map[string]string`) and `parseSanitizeConfig(raw string) (sanitizeConfig, error)` — handles both YAML file and inline `key=type` parsing, validates faker types against a known set
- [ ] 1.3 Create `sanitizer` struct with `fields`, `faker` fields and `newSanitizer(cfg sanitizeConfig, seed int64) *sanitizer` constructor — seed=0 is random, non-zero is deterministic
- [ ] 1.4 Implement `generate(fakerType string) string` method — switch dispatch to gofakeit functions for: `firstName`, `lastName`, `email`, `phone`, `address`, `companyName`, `uuid`
- [ ] 1.5 Implement `sanitizeRecord(data map[string]any)` method — recursive traversal of maps and arrays of maps, replaces matched string values

## Task 2: Unit tests for config parsing and sanitizer
- **Status:** pending
- **Depends on:** Task 1
- **Docs:** [implementation.md#sanitizer-tests](./implementation.md#sanitizer-tests)

### Subtasks
- [ ] 2.1 Create `sanitize_test.go` with tests for inline config parsing (valid input, malformed input, unknown faker types)
- [ ] 2.2 Add tests for YAML config parsing (create temp file, parse, verify)
- [ ] 2.3 Add tests for `sanitizeRecord`: flat map replacement, nested map recursion, array-of-maps recursion, non-string values left untouched
- [ ] 2.4 Add tests for seed determinism (same seed = same output) and seed=0 randomness (different output)

## Task 3: Export integration
- **Status:** pending
- **Depends on:** Task 1
- **Docs:** [implementation.md#export-integration](./implementation.md#export-integration)

### Subtasks
- [ ] 3.1 Add `--sanitize` (string) and `--seed` (int64) flags to the `export` subcommand in `main()`
- [ ] 3.2 Add `sanitizer *sanitizer` field to `exportConfig`
- [ ] 3.3 In `run()`, parse `--sanitize` flag and construct sanitizer if non-empty, set it on `exportConfig`
- [ ] 3.4 Thread sanitizer through `runExport` → `exportCollectionTree` → `exportSubCollectionTree` → `readAndExportCollection` / `readAndExportAggregated` — apply `sanitizeRecord` on each `docRecord.data` before calling `writeCollectionCSV`

## Task 4: Sanitize subcommand
- **Status:** pending
- **Depends on:** Task 1
- **Docs:** [implementation.md#sanitize-subcommand](./implementation.md#sanitize-subcommand)

### Subtasks
- [ ] 4.1 Register `sanitize` subcommand in `main()` with flags: `--config` (required), `--input`/`-i` (default `.`), `--output`/`-o` (required), `--seed` (default 0)
- [ ] 4.2 Implement `runSanitize` in `sanitize.go`: discover CSV files in input path, reuse/mirror `discoverCSVFiles` logic
- [ ] 4.3 For each CSV: read all rows, match column names against config (skip `__path__` and `__fs_types__`), replace matched cells via `sanitizer.generate()`, write to output directory preserving relative path structure
- [ ] 4.4 Print a summary of files processed and rows sanitized

## Task 5: Integration tests
- **Status:** pending
- **Depends on:** Task 3, Task 4
- **Docs:** [implementation.md#integration-tests](./implementation.md#integration-tests)

### Subtasks
- [ ] 5.1 In `integration_test.go`, add export+sanitize test: seed data with known PII, export with `--sanitize` and `--seed`, verify configured fields are replaced and non-configured fields are untouched, verify determinism with same seed
- [ ] 5.2 Add sanitize-subcommand test: create CSV with known content, run sanitize logic, verify output has replaced columns and preserved structure

## Task 6: Final verification
- **Status:** pending
- **Depends on:** Task 1, Task 2, Task 3, Task 4, Task 5

### Subtasks
- [ ] 6.1 Run `testing-process` skill to verify all tasks — full test suite, integration tests, edge cases
- [ ] 6.2 Run `documentation-process` skill to update README and any relevant docs
