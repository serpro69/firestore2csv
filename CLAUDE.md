# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build

```bash
go build -o firestore2csv .    # build binary
```

**Important! Running the application requires Google Application Default Credentials. Never run the app on your own, ask your human counterpart to do that if/when needed.**

## Architecture

Single-file Go CLI (`main.go`) using Cobra for command parsing. Connects to Firestore, iterates documents from specified (or all top-level) collections, and writes one CSV per collection.

Key flow: `main()` → Cobra `run()` → `resolveCollections()` → `exportCollection()` per collection → `formatValue()`/`convertForJSON()` for type conversion.

Data type handling: Firestore types are converted to CSV-friendly strings — timestamps to RFC3339Nano, arrays/maps to JSON, GeoPoints to `{"lat":..,"lng":..}`, bytes to base64, references to document paths.

## Testing

Unit tests (`main_test.go`) cover pure functions — no infrastructure needed:

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
