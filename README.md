# firestore2csv

A CLI tool to export Google Cloud Firestore collections to CSV files.

## Prerequisites

- Go 1.21+
- Google Cloud credentials configured via [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials):
  ```bash
  gcloud auth application-default login
  ```

## Usage

Run directly without building a binary:

```bash
go run . -p <project-id> [flags]
```

Or build and run:

```bash
go build -o firestore2csv .
./firestore2csv -p <project-id> [flags]
```

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--project` | `-p` | *(required)* | GCP project ID |
| `--database` | `-d` | `(default)` | Firestore database name |
| `--collections` | `-c` | *(all)* | Comma-separated collection names to export |
| `--limit` | `-l` | `0` (all) | Max documents per collection |
| `--output` | `-o` | `.` | Output directory for CSV files |

### Examples

Export all collections:

```bash
go run . -p my-project
```

Export specific collections with a row limit:

```bash
go run . -p my-project -c users,orders -l 100
```

Export from a named database to a custom directory:

```bash
go run . -p my-project -d my-db -o ./export
```

## Output Format

- One CSV file per collection, named `{collection}.csv`
- First column is `__document_id__` (Firestore document ID)
- Remaining columns are sorted alphabetically
- Columns are the union of all fields across documents in the collection

### Data type mapping

| Firestore Type | CSV Representation |
|---|---|
| String, Number, Boolean | Plain value |
| Null | Empty string |
| Timestamp | RFC3339Nano (`2024-01-15T10:30:00.123456789Z`) |
| Array | JSON string (`[1,"two",3]`) |
| Map | JSON string (`{"key":"value"}`) |
| GeoPoint | JSON string (`{"lat":12.34,"lng":56.78}`) |
| Bytes | Base64-encoded string |
| Reference | Document path (`projects/p/databases/d/documents/col/doc`) |
