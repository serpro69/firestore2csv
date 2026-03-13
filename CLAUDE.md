# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build

```bash
go build -o firestore2csv .    # build binary
```

**Important! Running the application requires Google Application Default Credentials. Never run the app on your own, ask your human counterpart to do that if/when needed.**

## Architecture

Go CLI using Cobra with three subcommands: `export`, `import`, and `sanitize`. Core export/import logic lives in `main.go`, sanitization logic in `sanitize.go`. Connection flags (`--project`/`-p`, `--emulator`/`-e`, `--database`) are shared across subcommands via `newFirestoreClient()`.

### Export

`main()` → `run()` → `runExport()` → `resolveCollections()` → `exportCollectionTree()` per collection → `writeCollectionCSV()`.

CSV format: first column is `__path__` (full document path, e.g. `users/alice/orders/order1`). Optional `--with-types` flag appends a `__fs_types__` column containing a JSON map of field→type labels.

Data type handling: Firestore types are converted to CSV-friendly strings — timestamps to RFC3339Nano, arrays/maps to JSON, GeoPoints to `{"lat":..,"lng":..}`, bytes to base64, references to document paths. The `typeLabel()` function maps Go types to labels (`string`, `bool`, `int`, `float`, `timestamp`, `geo`, `bytes`, `ref`, `array`, `map`).

### Import

`main()` → `runImportCmd()` → `runImport()` → `discoverCSVFiles()` → `parseCSVFile()` per CSV → Firestore writes with conflict handling.

Type reconstruction: if `__fs_types__` column is present, `castValue()` uses explicit type labels. Otherwise, `detectType()` applies heuristics (bool → timestamp → int → float → JSON → string).

Conflict strategies (`--on-conflict`): `skip` (don't overwrite existing), `overwrite` (Set), `merge` (Set with MergeAll), `fail` (abort if any doc exists). Supports `--dry-run` to preview without writing.

### Sanitization

**File:** `sanitize.go`

Data sanitization replaces PII in exported data with realistic fake values via `gofakeit/v7`. Available in two forms:

1. **`--sanitize` flag on `export`** — sanitizes during export, before CSV is written. Threaded through `exportCollectionTree` → `readAndExportCollection` / `readAndExportAggregated` via a `*sanitizer` parameter.
2. **`sanitize` subcommand** — standalone CSV-to-CSV transformation (`runSanitizeCmd` → `runSanitize` → `sanitizeCSVFile`). Discovers CSV files, replaces matched column values, writes to a separate output directory preserving path structure. Operates at column level only (no JSON-blob field matching).

Config parsing (`parseSanitizeConfig`): polymorphic — `.yaml`/`.yml` suffix loads a YAML file, otherwise parses inline `key=type` comma-separated pairs. Validates against a known set of faker types (`firstName`, `lastName`, `email`, `phone`, `address`, `companyName`, `uuid`).

Seed control: `--seed 0` (default) = crypto/rand (non-deterministic), non-zero = deterministic via PCG. Map keys and column indices are sorted before processing to ensure consistent RNG consumption order with seeded fakers.

## Testing

Unit tests (`main_test.go`, `sanitize_test.go`) cover pure functions — no infrastructure needed:

```bash
go test -v ./...
```

Integration tests (`integration_test.go`) run against a local Firestore emulator and are gated with `//go:build integration`. They require the [Firebase CLI](https://firebase.google.com/docs/cli) installed locally.

```bash
make test-integration    # starts emulator on port 8686, runs tests, stops emulator
make test-all            # unit + integration
```

To run integration tests manually:

```bash
firebase emulators:start --only firestore --project test-project &
FIRESTORE_EMULATOR_HOST=localhost:8686 go test -v -tags integration -count=1 ./...
```

The emulator port is configured in `firebase.json`.
