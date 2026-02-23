package main

import (
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/type/latlng"
)

var (
	project     = flag.String("project", "", "GCP project ID (required)")
	database    = flag.String("database", "(default)", "Firestore database name")
	collections = flag.String("collections", "", "Comma-separated collection names (default: all top-level)")
	limit       = flag.Int("limit", 0, "Max documents per collection (0 = all)")
	output      = flag.String("output", ".", "Output directory for CSV files")
)

func main() {
	flag.Parse()

	if *project == "" {
		fmt.Fprintf(os.Stderr, "Error: --project is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	if err := os.MkdirAll(*output, 0755); err != nil {
		log.Fatalf("Failed to create output directory %q: %v", *output, err)
	}

	ctx := context.Background()

	client, err := firestore.NewClientWithDatabase(ctx, *project, *database)
	if err != nil {
		log.Fatalf("Failed to create Firestore client: %v", err)
	}
	defer client.Close()

	collNames, err := resolveCollections(ctx, client, *collections)
	if err != nil {
		log.Fatalf("Failed to resolve collections: %v", err)
	}

	log.Printf("Exporting %d collection(s): %s", len(collNames), strings.Join(collNames, ", "))

	var failed []string
	for _, name := range collNames {
		if err := exportCollection(ctx, client, name, *limit, *output); err != nil {
			log.Printf("ERROR exporting %q: %v", name, err)
			failed = append(failed, name)
		}
	}

	if len(failed) > 0 {
		log.Fatalf("Export completed with errors in: %s", strings.Join(failed, ", "))
	}
	log.Println("Export completed successfully.")
}

func resolveCollections(ctx context.Context, client *firestore.Client, flagValue string) ([]string, error) {
	if flagValue != "" {
		parts := strings.Split(flagValue, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		return parts, nil
	}

	var names []string
	iter := client.Collections(ctx)
	for {
		colRef, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("listing collections: %w", err)
		}
		names = append(names, colRef.ID)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no collections found in database")
	}
	return names, nil
}

func exportCollection(ctx context.Context, client *firestore.Client, name string, limit int, outputDir string) error {
	log.Printf("Exporting collection %q...", name)

	colRef := client.Collection(name)
	query := colRef.Query
	if limit > 0 {
		query = query.Limit(limit)
	}

	iter := query.Documents(ctx)
	defer iter.Stop()

	fieldSet := make(map[string]struct{})
	type docRecord struct {
		id   string
		data map[string]interface{}
	}
	var docs []docRecord

	count := 0
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("reading collection %q: %w", name, err)
		}
		data := snap.Data()
		for k := range data {
			fieldSet[k] = struct{}{}
		}
		docs = append(docs, docRecord{id: snap.Ref.ID, data: data})
		count++
		if count%500 == 0 {
			log.Printf("  ...read %d documents from %q", count, name)
		}
	}

	if len(docs) == 0 {
		log.Printf("  Collection %q is empty, skipping.", name)
		return nil
	}
	log.Printf("  Read %d documents from %q with %d unique fields.", len(docs), name, len(fieldSet))

	// Build sorted header, prepend __document_id__
	fields := make([]string, 0, len(fieldSet))
	for k := range fieldSet {
		fields = append(fields, k)
	}
	sort.Strings(fields)
	headers := append([]string{"__document_id__"}, fields...)

	// Create CSV file
	filePath := filepath.Join(outputDir, name+".csv")
	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("creating file %s: %w", filePath, err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write(headers); err != nil {
		return fmt.Errorf("writing header: %w", err)
	}

	for _, doc := range docs {
		row := make([]string, len(headers))
		row[0] = doc.id
		for i, h := range fields {
			val, ok := doc.data[h]
			if !ok || val == nil {
				row[i+1] = ""
				continue
			}
			row[i+1] = formatValue(val)
		}
		if err := w.Write(row); err != nil {
			return fmt.Errorf("writing row: %w", err)
		}
	}

	log.Printf("  Wrote %s (%d rows)", filePath, len(docs))
	return nil
}

func formatValue(v interface{}) string {
	switch val := v.(type) {
	case nil:
		return ""
	case bool:
		if val {
			return "true"
		}
		return "false"
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case string:
		return val
	case time.Time:
		return val.Format(time.RFC3339Nano)
	case *latlng.LatLng:
		b, _ := json.Marshal(map[string]float64{
			"lat": val.GetLatitude(),
			"lng": val.GetLongitude(),
		})
		return string(b)
	case []byte:
		return base64.StdEncoding.EncodeToString(val)
	case *firestore.DocumentRef:
		return val.Path
	case []interface{}:
		b, _ := json.Marshal(convertForJSON(v))
		return string(b)
	case map[string]interface{}:
		b, _ := json.Marshal(convertForJSON(v))
		return string(b)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func convertForJSON(v interface{}) interface{} {
	switch val := v.(type) {
	case nil:
		return nil
	case bool, int64, float64, string:
		return val
	case time.Time:
		return val.Format(time.RFC3339Nano)
	case *latlng.LatLng:
		return map[string]float64{
			"lat": val.GetLatitude(),
			"lng": val.GetLongitude(),
		}
	case []byte:
		return base64.StdEncoding.EncodeToString(val)
	case *firestore.DocumentRef:
		return val.Path
	case []interface{}:
		out := make([]interface{}, len(val))
		for i, elem := range val {
			out[i] = convertForJSON(elem)
		}
		return out
	case map[string]interface{}:
		out := make(map[string]interface{}, len(val))
		for k, elem := range val {
			out[k] = convertForJSON(elem)
		}
		return out
	default:
		return fmt.Sprintf("%v", v)
	}
}
