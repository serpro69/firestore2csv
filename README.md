# firestore2csv

A CLI tool to export Google Cloud Firestore collections to CSV files.

## Prerequisites

- Go 1.21+
- Google Cloud credentials configured via [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials):
  ```bash
  gcloud auth application-default login
  ```

## Usage

Install and run:

```bash
go install github.com/serpro69/firestore2csv
firestore2csv -p <project-id> [flags]
```

Or build and run:

```bash
go build -o firestore2csv .
./firestore2csv -p <project-id> [flags]
```

Or run directly without installing/building a binary:

```bash
go run . -p <project-id> [flags]
```

### Flags

| Flag            | Short | Default      | Description                                          |
| --------------- | ----- | ------------ | ---------------------------------------------------- |
| `--project`     | `-p`  | _(required)_ | GCP project ID                                       |
| `--database`    | `-d`  | `(default)`  | Firestore database name                              |
| `--collections` | `-c`  | _(all)_      | Comma-separated top-level collection names to export |
| `--limit`       | `-l`  | `0` (all)    | Max documents per top-level collection               |
| `--child-limit` |       | `0` (all)    | Max documents per sub-collection                     |
| `--depth`       |       | `-1` (all)   | Max sub-collection depth (`0` = top-level only)      |
| `--output`      | `-o`  | `.`          | Output directory for CSV files                       |

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

Export only top-level collections (no sub-collections):

```bash
go run . -p my-project --depth 0
```

Export with one level of sub-collections, limiting sub-collection docs:

```bash
go run . -p my-project --depth 1 --child-limit 50
```

## Output Format

- One CSV file per collection, named `{collection}.csv`
- Sub-collections are exported into subdirectories mirroring the Firestore hierarchy
- First column is `__document_id__` (Firestore document ID)
- Remaining columns are sorted alphabetically
- Columns are the union of all fields across documents in the collection

### Sub-collections

Sub-collections are automatically discovered and exported recursively. Documents
from the same sub-collection across different parent documents are aggregated
into a single CSV file. Virtual documents (documents that exist only as
containers for sub-collections, with no fields of their own) are also traversed.

For example, if `users/alice` and `users/bob` both have an `orders`
sub-collection, all orders are written to `users/orders.csv`.

```
output/
  users.csv
  users/
    orders.csv
    orders/
      items.csv
  products.csv
```

Use `--depth` to control how deep to recurse (`0` = top-level only, `1` = one
level of sub-collections, `-1` = unlimited).

### Data type mapping

| Firestore Type          | CSV Representation                                         |
| ----------------------- | ---------------------------------------------------------- |
| String, Number, Boolean | Plain value                                                |
| Null                    | Empty string                                               |
| Timestamp               | RFC3339Nano (`2024-01-15T10:30:00.123456789Z`)             |
| Array                   | JSON string (`[1,"two",3]`)                                |
| Map                     | JSON string (`{"key":"value"}`)                            |
| GeoPoint                | JSON string (`{"lat":12.34,"lng":56.78}`)                  |
| Bytes                   | Base64-encoded string                                      |
| Reference               | Document path (`projects/p/databases/d/documents/col/doc`) |

## Testing

### Unit tests

Unit tests cover pure functions (value formatting, CSV writing, etc.) and require no external services:

```bash
go test -v ./...
```

### Integration tests

Integration tests run against a local [Firebase Firestore Emulator](https://firebase.google.com/docs/emulator-suite) and exercise the full export pipeline — collection resolution, recursive sub-collection export, depth/limit controls, and CSV output.

**Prerequisites:** [Firebase CLI](https://firebase.google.com/docs/cli) (`npm install -g firebase-tools`)

Run with Make (starts and stops the emulator automatically):

```bash
make test-integration
```

Or run all tests (unit + integration):

```bash
make test-all
```

To run manually:

```bash
firebase emulators:start --only firestore --project test-project &
FIRESTORE_EMULATOR_HOST=localhost:8686 go test -v -tags integration -count=1 ./...
```
