package main

import (
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/type/latlng"
)

type exportResult struct {
	collection string
	docCount   int
	fieldCount int
	filePath   string
	err        error
}

var (
	cyan  = color.New(color.FgCyan, color.Bold).SprintFunc()
	green = color.New(color.FgGreen, color.Bold).SprintFunc()
	red   = color.New(color.FgRed, color.Bold).SprintFunc()
	bold  = color.New(color.Bold).SprintFunc()
	faint = color.New(color.Faint).SprintFunc()
)

func printInfo(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "%s  %s\n", cyan("INFO"), fmt.Sprintf(format, a...))
}

func printOK(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "  %s  %s\n", green("✓"), fmt.Sprintf(format, a...))
}

func printErr(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "%s %s\n", red("ERROR"), fmt.Sprintf(format, a...))
}

// fmtInt formats an integer with comma thousands separators.
func fmtInt(n int) string {
	s := strconv.Itoa(n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	pre := len(s) % 3
	if pre > 0 {
		b.WriteString(s[:pre])
	}
	for i := pre; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

// spinner provides a simple animated spinner for terminal output.
type spinner struct {
	mu     sync.Mutex
	suffix string
	done   chan struct{}
}

var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

func newSpinner(suffix string) *spinner {
	return &spinner{suffix: suffix, done: make(chan struct{})}
}

func (s *spinner) SetSuffix(suffix string) {
	s.mu.Lock()
	s.suffix = suffix
	s.mu.Unlock()
}

func (s *spinner) Start() {
	go func() {
		i := 0
		for {
			select {
			case <-s.done:
				return
			default:
				s.mu.Lock()
				suffix := s.suffix
				s.mu.Unlock()
				fmt.Fprintf(os.Stderr, "\r\033[K%s %s", cyan(spinnerFrames[i%len(spinnerFrames)]), suffix)
				i++
				time.Sleep(80 * time.Millisecond)
			}
		}
	}()
}

func (s *spinner) Stop() {
	close(s.done)
	fmt.Fprintf(os.Stderr, "\r\033[K")
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "firestore2csv",
		Short: "Export Firestore collections to CSV files",
		Long: `Export Firestore collections to CSV files.

Each collection is written to a separate CSV file. The first column is always
__document_id__, and remaining columns are the union of all fields across
documents in that collection, sorted alphabetically.

Complex types (arrays, maps) are stored as JSON strings. Timestamps use
RFC3339 format. Authentication uses Google Application Default Credentials.`,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE:          run,
	}

	f := rootCmd.Flags()
	f.StringP("project", "p", "", "GCP project ID (required)")
	f.StringP("database", "d", "(default)", "Firestore database name")
	f.StringP("collections", "c", "", "Comma-separated collection names (default: all top-level)")
	f.IntP("limit", "l", 0, "Max documents per collection (0 = all)")
	f.StringP("output", "o", ".", "Output directory for CSV files")

	rootCmd.MarkFlagRequired("project")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "\n%s %s\n", red("ERROR"), err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	f := cmd.Flags()
	project, _ := f.GetString("project")
	database, _ := f.GetString("database")
	collections, _ := f.GetString("collections")
	limit, _ := f.GetInt("limit")
	output, _ := f.GetString("output")

	fmt.Fprintln(os.Stderr)
	printInfo("Connecting to project %s (database: %s)", bold(project), bold(database))

	if err := os.MkdirAll(output, 0755); err != nil {
		return fmt.Errorf("failed to create output directory %q: %w", output, err)
	}

	ctx := context.Background()
	client, err := firestore.NewClientWithDatabase(ctx, project, database)
	if err != nil {
		return fmt.Errorf("failed to create Firestore client: %w", err)
	}
	defer client.Close()

	collNames, err := resolveCollections(ctx, client, collections)
	if err != nil {
		return fmt.Errorf("failed to resolve collections: %w", err)
	}

	printInfo("Found %d collection(s): %s", len(collNames), strings.Join(collNames, ", "))
	fmt.Fprintln(os.Stderr)

	var results []exportResult
	for _, name := range collNames {
		r := exportCollection(ctx, client, name, limit, output)
		results = append(results, r)
	}

	printSummaryTable(results)

	var failed []string
	for _, r := range results {
		if r.err != nil {
			failed = append(failed, r.collection)
		}
	}

	if len(failed) > 0 {
		fmt.Fprintf(os.Stderr, "\n%s Export completed with %d error(s). Failed: %s\n",
			red("FAILED"), len(failed), strings.Join(failed, ", "))
		return fmt.Errorf("export failed for %d collection(s)", len(failed))
	}

	fmt.Fprintf(os.Stderr, "\n%s All %d collection(s) exported successfully.\n",
		green("✓"), len(results))
	return nil
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

func exportCollection(ctx context.Context, client *firestore.Client, name string, limit int, outputDir string) exportResult {
	sp := newSpinner(fmt.Sprintf("Reading %q... 0 documents", name))
	sp.Start()

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
		data map[string]any
	}
	var docs []docRecord

	count := 0
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			sp.Stop()
			printErr("Failed to export %q: %v", name, err)
			return exportResult{collection: name, err: err}
		}
		data := snap.Data()
		for k := range data {
			fieldSet[k] = struct{}{}
		}
		docs = append(docs, docRecord{id: snap.Ref.ID, data: data})
		count++
		sp.SetSuffix(fmt.Sprintf("Reading %q... %s documents", name, fmtInt(count)))
	}

	sp.Stop()

	if len(docs) == 0 {
		printInfo("Collection %q is empty, skipping.", name)
		return exportResult{collection: name}
	}

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
		printErr("Failed to export %q: %v", name, err)
		return exportResult{collection: name, err: fmt.Errorf("creating file %s: %w", filePath, err)}
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write(headers); err != nil {
		printErr("Failed to export %q: %v", name, err)
		return exportResult{collection: name, err: fmt.Errorf("writing header: %w", err)}
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
			printErr("Failed to export %q: %v", name, err)
			return exportResult{collection: name, err: fmt.Errorf("writing row: %w", err)}
		}
	}

	printOK("Exported %q — %s docs, %d fields → %s", name, fmtInt(len(docs)), len(fieldSet), filePath)

	return exportResult{
		collection: name,
		docCount:   len(docs),
		fieldCount: len(fieldSet),
		filePath:   filePath,
	}
}

func printSummaryTable(results []exportResult) {
	if len(results) == 0 {
		return
	}

	// Calculate column widths
	colW, docW, fldW, fileW := len("Collection"), len("Docs"), len("Fields"), len("Output File")
	rows := make([][]string, len(results))
	for i, r := range results {
		fp := r.filePath
		if fp == "" {
			fp = "-"
		}
		docs := fmtInt(r.docCount)
		fields := fmtInt(r.fieldCount)
		rows[i] = []string{r.collection, docs, fields, fp}
		if len(r.collection) > colW {
			colW = len(r.collection)
		}
		if len(docs) > docW {
			docW = len(docs)
		}
		if len(fields) > fldW {
			fldW = len(fields)
		}
		if len(fp) > fileW {
			fileW = len(fp)
		}
	}

	fmt.Fprintln(os.Stderr)
	// Header
	fmt.Fprintf(os.Stderr, " %-*s  %*s  %*s  %-*s\n",
		colW, bold("Collection"), docW, bold("Docs"), fldW, bold("Fields"), fileW, bold("Output File"))
	// Separator
	fmt.Fprintf(os.Stderr, " %s  %s  %s  %s\n",
		faint(strings.Repeat("─", colW)), faint(strings.Repeat("─", docW)), faint(strings.Repeat("─", fldW)), faint(strings.Repeat("─", fileW)))
	// Rows
	for _, row := range rows {
		fmt.Fprintf(os.Stderr, " %-*s  %*s  %*s  %-*s\n",
			colW, row[0], docW, row[1], fldW, row[2], fileW, row[3])
	}
}

func formatValue(v any) string {
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
	case []any:
		b, _ := json.Marshal(convertForJSON(v))
		return string(b)
	case map[string]any:
		b, _ := json.Marshal(convertForJSON(v))
		return string(b)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func convertForJSON(v any) any {
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
	case []any:
		out := make([]any, len(val))
		for i, elem := range val {
			out[i] = convertForJSON(elem)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(val))
		for k, elem := range val {
			out[k] = convertForJSON(elem)
		}
		return out
	default:
		return fmt.Sprintf("%v", v)
	}
}
