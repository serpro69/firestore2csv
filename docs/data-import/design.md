# Import Feature — Design

## Overview

Add the ability to import (write) CSV data into Firestore, complementing the existing export functionality. The primary use case is exporting from a dev/remote Firestore database and importing into a local emulator instance for local development and testing.

## CLI Structure Refactor

The current root command is replaced by two subcommands:

```
firestore2csv export [flags]
firestore2csv import [flags]
```

Running `firestore2csv` with no subcommand prints help/usage.

### Shared Flags (both subcommands)

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--project` | `-p` | (none) | GCP project ID (remote Firestore) |
| `--emulator` | `-e` | (none) | Emulator host (e.g. `localhost:8686`) |
| `--database` | `-d` | `(default)` | Firestore database name |

**Validation:** Exactly one of `--project` or `--emulator` must be provided. When `--emulator` is used, the tool sets `FIRESTORE_EMULATOR_HOST` internally and uses a placeholder project ID.

### Export Flags

All existing flags remain (`--collections`, `--limit`, `--child-limit`, `--depth`, `--output`), plus:

| Flag | Default | Description |
|------|---------|-------------|
| `--with-types` | `false` | Include `__fs_types__` column in CSV output |

### Import Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--input` | `-i` | `.` | Repeatable. Each value is a directory (processed recursively) or a file (processed directly) |
| `--on-conflict` | | `skip` | Conflict strategy: `overwrite`, `merge`, `skip`, `fail` |
| `--dry-run` | | `false` | Validate and report without writing to Firestore |

## Export Format Changes

### `__path__` Column (replaces `__document_id__`)

The first column changes from `__document_id__` to `__path__`. It contains the full Firestore document path.

**Top-level collection (`users.csv`):**
```csv
__path__,name,age
users/alice,Alice,30
users/bob,Bob,25
```

**Sub-collection (`users/orders.csv`):**
```csv
__path__,item,total
users/alice/orders/order1,Widget,42.50
users/bob/orders/order3,Widget,42.50
```

This is a breaking change from the previous `__document_id__` format. No backward compatibility is maintained (pre-release).

### `__fs_types__` Column (opt-in)

When `--with-types` is passed, an additional last column `__fs_types__` is appended. Each cell contains a minified JSON object mapping **every** field name to its Firestore type.

Supported type labels:

| Label | Firestore Type |
|-------|---------------|
| `string` | String |
| `bool` | Boolean |
| `int` | Integer (int64) |
| `float` | Float (float64) |
| `timestamp` | Timestamp |
| `geo` | GeoPoint |
| `bytes` | Bytes (base64-encoded in CSV) |
| `ref` | DocumentReference (path string in CSV) |
| `array` | Array (JSON string in CSV) |
| `map` | Map (JSON string in CSV) |

**Example with types:**
```csv
__path__,name,age,isActive,__fs_types__
users/alice,Alice,30,true,{"name":"string","age":"int","isActive":"bool"}
users/bob,Bob,forty-two,false,{"name":"string","age":"string","isActive":"bool"}
```

All fields are included in the type map, including strings. This removes ambiguity and makes manual editing straightforward.

## Import Flow

### Step 1: Discover CSVs

Process each `--input` value:
- If it is a directory, recursively find all `.csv` files within it.
- If it is a file, use it directly.

### Step 2: Parse Each CSV

For each CSV file, process rows:

1. Read `__path__` to determine the full Firestore document path. Split it: the last segment is the document ID, everything before it is the collection path.
2. For each data field in the row, determine its Firestore type:
   - **If `__fs_types__` column exists and is non-empty for this row:** use the declared type to cast the CSV string value to the correct Firestore type.
   - **If `__fs_types__` is absent or empty:** apply heuristic detection (see below).

### Step 3: Heuristic Type Detection

When no explicit type information is available, apply these rules in order:

1. Empty string → `nil` (field omitted)
2. `true` or `false` (case-sensitive) → `bool`
3. Parses as RFC3339Nano → `timestamp`
4. Parses as integer (no decimal point) → `int`
5. Parses as float → `float`
6. Parses as JSON object `{...}` → `map`
7. Parses as JSON array `[...]` → `array`
8. Everything else → `string`

### Step 4: Handle Conflicts

Based on `--on-conflict`:

- **`skip`** (default): Check if the document exists. If yes, skip it and log a notice. If no, write it.
- **`overwrite`**: Use Firestore `Set` to fully replace the document.
- **`merge`**: Use Firestore `Set` with `MergeAll` to update only the fields present in the CSV.
- **`fail`**: Check existence of all documents first. If any conflict is found, abort the entire import before writing anything.

### Step 5: Write to Firestore

For each parsed document, perform the Firestore write operation (unless `--dry-run` is set).

### Step 6: Dry-Run Mode

When `--dry-run` is enabled, execute steps 1–4 (including conflict detection) but skip all Firestore writes. Print a summary:
- Collections found
- Document counts per collection
- Conflicts detected (with document paths)
- Type parsing issues or warnings

### Step 7: Progress and Summary

During import, print progress per collection (similar to export). At the end, print a summary table showing:
- Documents written
- Documents skipped (with reason)
- Errors encountered

## Sub-collection Handling

Sub-collections are fully derived from the `__path__` column, not from the CSV file's location in the directory tree. The file's directory position is irrelevant to import — only `__path__` matters.

For example, `users/alice/orders/order1` in `__path__` creates document `order1` in the `orders` sub-collection under `users/alice`, regardless of which CSV file contains the row.

This means a single flat CSV with all documents (top-level and nested) would work just as well as a directory tree of CSVs — the `__path__` column is the source of truth.
