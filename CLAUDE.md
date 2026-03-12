# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
go build -o firestore2csv .    # build binary
go run . -p <project-id>       # run without building
```

Requires Google Application Default Credentials (`gcloud auth application-default login`).

## Architecture

Single-file Go CLI (`main.go`) using Cobra for command parsing. Connects to Firestore, iterates documents from specified (or all top-level) collections, and writes one CSV per collection.

Key flow: `main()` → Cobra `run()` → `resolveCollections()` → `exportCollection()` per collection → `formatValue()`/`convertForJSON()` for type conversion.

Data type handling: Firestore types are converted to CSV-friendly strings — timestamps to RFC3339Nano, arrays/maps to JSON, GeoPoints to `{"lat":..,"lng":..}`, bytes to base64, references to document paths.

No tests exist currently.
