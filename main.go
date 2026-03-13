package main

import (
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
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

// defaultEmulatorProject is the project ID used when connecting to an emulator
// without an explicit --project flag.
const defaultEmulatorProject = "emulator-project"

type docRecord struct {
	path string
	data map[string]any
}

type exportResult struct {
	collection string
	depth      int
	docCount   int
	fieldCount int
	filePath   string
	err        error
}

func buildVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	// Local builds have VCS info; prefer commit hash
	var revision, dirty string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "-dirty"
			}
		}
	}
	if revision != "" {
		if len(revision) > 7 {
			revision = revision[:7]
		}
		return revision + dirty
	}
	// go install module@version sets Main.Version (e.g. "v1.2.3")
	if v := info.Main.Version; v != "" && v != "(devel)" {
		return v
	}
	return "dev"
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

// documentPath extracts the document path from a Firestore DocumentRef.
// snap.Ref.Path returns "projects/{project}/databases/{db}/documents/{path}";
// this function returns just the "{path}" portion.
func documentPath(ref *firestore.DocumentRef) string {
	full := ref.Path
	const marker = "/documents/"
	if i := strings.Index(full, marker); i >= 0 {
		return full[i+len(marker):]
	}
	return full
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
		Short: "Export and import Firestore collections as CSV files",
		Long: `Export and import Firestore collections as CSV files.

Use 'firestore2csv export' to export collections to CSV, or
'firestore2csv import' to import CSV files into Firestore.

Run 'firestore2csv <command> --help' for details on each command.`,
		Version:       buildVersion(),
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	// Shared flags on root (inherited by subcommands)
	pf := rootCmd.PersistentFlags()
	pf.StringP("project", "p", "", "GCP project ID")
	pf.StringP("emulator", "e", "", "Firestore emulator host (e.g. localhost:8686)")
	pf.StringP("database", "d", "(default)", "Firestore database name")

	// Export subcommand
	exportCmd := &cobra.Command{
		Use:   "export",
		Short: "Export Firestore collections to CSV files",
		Long: `Export Firestore collections to CSV files.

Each collection is written to a separate CSV file. The first column is always
__path__ (the full Firestore document path), and remaining columns are the
union of all fields across documents in that collection, sorted alphabetically.

Sub-collections are automatically discovered and exported recursively. Output
files are organized in a directory structure mirroring the collection hierarchy
(e.g. users.csv, users/orders.csv). Use --depth to limit recursion depth.

Complex types (arrays, maps) are stored as JSON strings. Timestamps use
RFC3339 format. Authentication uses Google Application Default Credentials.`,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE:          run,
	}

	ef := exportCmd.Flags()
	ef.StringP("collections", "c", "", "Comma-separated collection names (default: all top-level)")
	ef.IntP("limit", "l", 0, "Max documents per top-level collection (0 = all)")
	ef.Int("child-limit", 0, "Max documents per sub-collection (0 = all)")
	ef.Int("depth", -1, "Max sub-collection depth (-1 = unlimited, 0 = top-level only)")
	ef.StringP("output", "o", ".", "Output directory for CSV files")
	ef.Bool("with-types", false, "Include __fs_types__ column with Firestore type metadata")
	ef.String("sanitize", "", "Sanitize fields: inline key=type pairs or path to YAML config file")
	ef.Int64("seed", 0, "Random seed for sanitization (0 = random, non-zero = deterministic)")

	// Import subcommand
	importCmd := &cobra.Command{
		Use:   "import",
		Short: "Import CSV files into Firestore",
		Long: `Import CSV files into Firestore.

Reads CSV files (exported by 'firestore2csv export') and writes the data
back into Firestore. The __path__ column determines the document location.

Use --on-conflict to control behavior when documents already exist.
Use --dry-run to validate without writing.`,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE:          runImportCmd,
	}

	imf := importCmd.Flags()
	imf.StringSliceP("input", "i", []string{"."}, "Input files or directories (repeatable)")
	imf.String("on-conflict", "skip", "Conflict strategy: skip, overwrite, merge, fail")
	imf.Bool("dry-run", false, "Validate and report without writing to Firestore")

	// Sanitize subcommand
	sanitizeCmd := &cobra.Command{
		Use:   "sanitize",
		Short: "Sanitize PII in previously exported CSV files",
		Long: `Sanitize PII in previously exported CSV files.

Reads CSV files and replaces values in configured columns with realistic
fake data generated by gofakeit. Output is written to a separate directory,
preserving the relative directory structure of the input.

Special columns (__path__, __fs_types__) are never modified.`,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE:          runSanitizeCmd,
	}

	sf := sanitizeCmd.Flags()
	sf.String("config", "", "Sanitization config: inline key=type pairs or path to YAML file (required)")
	sf.StringP("input", "i", ".", "Input file or directory containing CSV files")
	sf.StringP("output", "o", "", "Output directory for sanitized CSV files (required)")
	sf.Int64("seed", 0, "Random seed (0 = random, non-zero = deterministic)")
	_ = sanitizeCmd.MarkFlagRequired("config")
	_ = sanitizeCmd.MarkFlagRequired("output")

	rootCmd.AddCommand(exportCmd)
	rootCmd.AddCommand(importCmd)
	rootCmd.AddCommand(sanitizeCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "\n%s %s\n", red("ERROR"), err)
		os.Exit(1)
	}
}

type exportConfig struct {
	project     string
	database    string
	emulator    string
	collections string
	limit       int
	childLimit  int
	maxDepth    int
	output      string
	withTypes   bool
	sanitizer   *sanitizer
}

// validateConnectionFlags ensures at least one of --project or --emulator is provided.
func validateConnectionFlags(cmd *cobra.Command) (project, database, emulator string, err error) {
	f := cmd.Flags()
	project, _ = f.GetString("project")
	emulator, _ = f.GetString("emulator")
	database, _ = f.GetString("database")

	if project == "" && emulator == "" {
		return "", "", "", fmt.Errorf("at least one of --project or --emulator must be provided")
	}
	return project, database, emulator, nil
}

// newFirestoreClient creates a Firestore client, handling emulator configuration.
func newFirestoreClient(ctx context.Context, project, database, emulator string) (*firestore.Client, error) {
	if emulator != "" {
		os.Setenv("FIRESTORE_EMULATOR_HOST", emulator)
		if project == "" {
			project = defaultEmulatorProject
		}
	}
	return firestore.NewClientWithDatabase(ctx, project, database)
}

func run(cmd *cobra.Command, args []string) error {
	project, database, emulator, err := validateConnectionFlags(cmd)
	if err != nil {
		return err
	}

	f := cmd.Flags()
	collections, _ := f.GetString("collections")
	limit, _ := f.GetInt("limit")
	childLimit, _ := f.GetInt("child-limit")
	maxDepth, _ := f.GetInt("depth")
	output, _ := f.GetString("output")
	withTypes, _ := f.GetBool("with-types")
	sanitizeFlag, _ := f.GetString("sanitize")
	seed, _ := f.GetInt64("seed")

	var san *sanitizer
	if sanitizeFlag != "" {
		cfg, err := parseSanitizeConfig(sanitizeFlag)
		if err != nil {
			return fmt.Errorf("invalid --sanitize config: %w", err)
		}
		san = newSanitizer(cfg, seed)
	}

	return runExport(exportConfig{
		project:     project,
		database:    database,
		emulator:    emulator,
		collections: collections,
		limit:       limit,
		childLimit:  childLimit,
		maxDepth:    maxDepth,
		output:      output,
		withTypes:   withTypes,
		sanitizer:   san,
	})
}

func runExport(cfg exportConfig) error {
	fmt.Fprintln(os.Stderr)
	displayProject := cfg.project
	if cfg.emulator != "" {
		displayProject = fmt.Sprintf("emulator @ %s", cfg.emulator)
	}
	printInfo("Connecting to %s (database: %s)", bold(displayProject), bold(cfg.database))

	if err := os.MkdirAll(cfg.output, 0755); err != nil {
		return fmt.Errorf("failed to create output directory %q: %w", cfg.output, err)
	}

	ctx := context.Background()
	client, err := newFirestoreClient(ctx, cfg.project, cfg.database, cfg.emulator)
	if err != nil {
		return fmt.Errorf("failed to create Firestore client: %w", err)
	}
	defer client.Close()

	collNames, err := resolveCollections(ctx, client, cfg.collections)
	if err != nil {
		return fmt.Errorf("failed to resolve collections: %w", err)
	}

	printInfo("Found %d collection(s): %s", len(collNames), strings.Join(collNames, ", "))
	fmt.Fprintln(os.Stderr)

	var results []exportResult
	for _, name := range collNames {
		results = append(results, exportCollectionTree(ctx, client, name, cfg.limit, cfg.childLimit, cfg.maxDepth, cfg.output, cfg.withTypes, cfg.sanitizer)...)
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

// exportCollectionTree exports a top-level collection and recursively exports its sub-collections.
func exportCollectionTree(ctx context.Context, client *firestore.Client, name string, limit, childLimit, maxDepth int, outputDir string, withTypes bool, san *sanitizer) []exportResult {
	colRef := client.Collection(name)
	recurse := maxDepth != 0

	result, docRefs := readAndExportCollection(ctx, colRef, name, 0, limit, recurse, outputDir, withTypes, san)
	results := []exportResult{result}
	if result.err != nil || !recurse {
		return results
	}

	subCols := discoverSubCollections(ctx, docRefs)
	for _, subName := range sortedKeys(subCols) {
		parentRefs := subCols[subName]
		displayPath := name + "/" + subName
		nextDepth := maxDepth
		if nextDepth > 0 {
			nextDepth--
		}
		results = append(results, exportSubCollectionTree(ctx, parentRefs, subName, displayPath, 1, nextDepth, childLimit, outputDir, withTypes, san)...)
	}

	return results
}

// exportSubCollectionTree recursively exports an aggregated sub-collection and its children.
func exportSubCollectionTree(ctx context.Context, parentRefs []*firestore.DocumentRef, subColName, displayPath string, depth, maxDepth, childLimit int, outputDir string, withTypes bool, san *sanitizer) []exportResult {
	recurse := maxDepth != 0

	result, docRefs := readAndExportAggregated(ctx, parentRefs, subColName, displayPath, depth, childLimit, recurse, outputDir, withTypes, san)
	results := []exportResult{result}
	if result.err != nil || !recurse {
		return results
	}

	subCols := discoverSubCollections(ctx, docRefs)
	for _, subSubName := range sortedKeys(subCols) {
		refs := subCols[subSubName]
		subDisplayPath := displayPath + "/" + subSubName
		nextDepth := maxDepth
		if nextDepth > 0 {
			nextDepth--
		}
		results = append(results, exportSubCollectionTree(ctx, refs, subSubName, subDisplayPath, depth+1, nextDepth, childLimit, outputDir, withTypes, san)...)
	}

	return results
}

// readAndExportCollection reads documents from a single collection ref and writes a CSV.
// If recurse is true, it returns the document refs for sub-collection discovery.
func readAndExportCollection(ctx context.Context, colRef *firestore.CollectionRef, displayPath string, depth, limit int, recurse bool, outputDir string, withTypes bool, san *sanitizer) (exportResult, []*firestore.DocumentRef) {
	sp := newSpinner(fmt.Sprintf("Reading %q... 0 documents", displayPath))
	sp.Start()

	query := colRef.Query
	if limit > 0 {
		query = query.Limit(limit)
	}

	iter := query.Documents(ctx)
	defer iter.Stop()

	fieldSet := make(map[string]struct{})
	var docs []docRecord
	var docRefs []*firestore.DocumentRef

	count := 0
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			sp.Stop()
			printErr("Failed to export %q: %v", displayPath, err)
			return exportResult{collection: displayPath, depth: depth, err: err}, nil
		}
		data := snap.Data()
		for k := range data {
			fieldSet[k] = struct{}{}
		}
		docs = append(docs, docRecord{path: documentPath(snap.Ref), data: data})
		if recurse {
			docRefs = append(docRefs, snap.Ref)
		}
		count++
		sp.SetSuffix(fmt.Sprintf("Reading %q... %s documents", displayPath, fmtInt(count)))
	}

	sp.Stop()

	if len(docs) == 0 {
		// Even if there are no documents with data, there may be virtual
		// documents that act as containers for sub-collections. List document
		// refs so the caller can still discover sub-collections.
		if recurse {
			refIter := colRef.DocumentRefs(ctx)
			for {
				ref, err := refIter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					printErr("Failed to list document refs for %q: %v", displayPath, err)
					break
				}
				docRefs = append(docRefs, ref)
			}
		}
		if len(docRefs) == 0 {
			printInfo("Collection %q is empty, skipping.", displayPath)
		} else {
			printInfo("Collection %q has no documents with data, checking sub-collections...", displayPath)
		}
		return exportResult{collection: displayPath, depth: depth}, docRefs
	}

	if san != nil {
		for i := range docs {
			san.sanitizeRecord(docs[i].data)
		}
	}

	filePath, err := writeCollectionCSV(docs, fieldSet, displayPath, outputDir, withTypes)
	if err != nil {
		printErr("Failed to export %q: %v", displayPath, err)
		return exportResult{collection: displayPath, depth: depth, err: err}, nil
	}

	printOK("Exported %q — %s docs, %d fields → %s", displayPath, fmtInt(len(docs)), len(fieldSet), filePath)

	return exportResult{
		collection: displayPath,
		depth:      depth,
		docCount:   len(docs),
		fieldCount: len(fieldSet),
		filePath:   filePath,
	}, docRefs
}

// readAndExportAggregated reads documents from a sub-collection across multiple parent documents
// and writes them into a single CSV.
func readAndExportAggregated(ctx context.Context, parentRefs []*firestore.DocumentRef, subColName, displayPath string, depth, childLimit int, recurse bool, outputDir string, withTypes bool, san *sanitizer) (exportResult, []*firestore.DocumentRef) {
	sp := newSpinner(fmt.Sprintf("Reading %q... 0 documents", displayPath))
	sp.Start()

	fieldSet := make(map[string]struct{})
	var docs []docRecord
	var docRefs []*firestore.DocumentRef

	count := 0
	for _, parentRef := range parentRefs {
		colRef := parentRef.Collection(subColName)
		query := colRef.Query
		if childLimit > 0 {
			query = query.Limit(childLimit)
		}

		iter := query.Documents(ctx)
		for {
			snap, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				iter.Stop()
				sp.Stop()
				printErr("Failed to export %q: %v", displayPath, err)
				return exportResult{collection: displayPath, depth: depth, err: err}, nil
			}
			data := snap.Data()
			for k := range data {
				fieldSet[k] = struct{}{}
			}
			docs = append(docs, docRecord{path: documentPath(snap.Ref), data: data})
			if recurse {
				docRefs = append(docRefs, snap.Ref)
			}
			count++
			sp.SetSuffix(fmt.Sprintf("Reading %q... %s documents", displayPath, fmtInt(count)))
		}
		iter.Stop()
	}

	sp.Stop()

	if len(docs) == 0 {
		// Even if there are no documents with data, there may be virtual
		// documents that act as containers for sub-collections.
		if recurse {
			for _, parentRef := range parentRefs {
				colRef := parentRef.Collection(subColName)
				refIter := colRef.DocumentRefs(ctx)
				for {
					ref, err := refIter.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						printErr("Failed to list document refs for %q: %v", displayPath, err)
						break
					}
					docRefs = append(docRefs, ref)
				}
			}
		}
		if len(docRefs) == 0 {
			printInfo("Collection %q is empty, skipping.", displayPath)
		} else {
			printInfo("Collection %q has no documents with data, checking sub-collections...", displayPath)
		}
		return exportResult{collection: displayPath, depth: depth}, docRefs
	}

	if san != nil {
		for i := range docs {
			san.sanitizeRecord(docs[i].data)
		}
	}

	filePath, err := writeCollectionCSV(docs, fieldSet, displayPath, outputDir, withTypes)
	if err != nil {
		printErr("Failed to export %q: %v", displayPath, err)
		return exportResult{collection: displayPath, depth: depth, err: err}, nil
	}

	printOK("Exported %q — %s docs, %d fields → %s", displayPath, fmtInt(len(docs)), len(fieldSet), filePath)

	return exportResult{
		collection: displayPath,
		depth:      depth,
		docCount:   len(docs),
		fieldCount: len(fieldSet),
		filePath:   filePath,
	}, docRefs
}

// discoverSubCollections finds all sub-collections across the given document refs.
// Returns a map of sub-collection name → parent document refs that contain it.
func discoverSubCollections(ctx context.Context, docRefs []*firestore.DocumentRef) map[string][]*firestore.DocumentRef {
	subCols := make(map[string][]*firestore.DocumentRef)
	for _, ref := range docRefs {
		iter := ref.Collections(ctx)
		for {
			colRef, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				printErr("Failed to list sub-collections for %q: %v", ref.Path, err)
				break
			}
			subCols[colRef.ID] = append(subCols[colRef.ID], ref)
		}
	}
	return subCols
}

// writeCollectionCSV writes document records to a CSV file.
func writeCollectionCSV(docs []docRecord, fieldSet map[string]struct{}, displayPath, outputDir string, withTypes bool) (string, error) {
	fields := make([]string, 0, len(fieldSet))
	for k := range fieldSet {
		fields = append(fields, k)
	}
	sort.Strings(fields)
	headers := append([]string{"__path__"}, fields...)
	if withTypes {
		headers = append(headers, "__fs_types__")
	}

	filePath := filepath.Join(outputDir, filepath.FromSlash(displayPath)+".csv")
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return "", fmt.Errorf("creating directory for %s: %w", filePath, err)
	}

	f, err := os.Create(filePath)
	if err != nil {
		return "", fmt.Errorf("creating file %s: %w", filePath, err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write(headers); err != nil {
		return "", fmt.Errorf("writing header: %w", err)
	}

	for _, doc := range docs {
		row := make([]string, len(headers))
		row[0] = doc.path
		typeMap := make(map[string]string, len(fields))
		for i, h := range fields {
			val, ok := doc.data[h]
			if !ok || val == nil {
				row[i+1] = ""
				continue
			}
			row[i+1] = formatValue(val)
			if withTypes {
				typeMap[h] = typeLabel(val)
			}
		}
		if withTypes {
			b, _ := json.Marshal(typeMap)
			row[len(row)-1] = string(b)
		}
		if err := w.Write(row); err != nil {
			return "", fmt.Errorf("writing row: %w", err)
		}
	}

	return filePath, nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
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
		indent := strings.Repeat("  ", r.depth)
		displayName := indent + r.collection
		rows[i] = []string{displayName, docs, fields, fp}
		if len(displayName) > colW {
			colW = len(displayName)
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

// typeLabel returns the Firestore type label for a value.
func typeLabel(v any) string {
	switch v.(type) {
	case bool:
		return "bool"
	case int64:
		return "int"
	case float64:
		return "float"
	case string:
		return "string"
	case time.Time:
		return "timestamp"
	case *latlng.LatLng:
		return "geo"
	case []byte:
		return "bytes"
	case *firestore.DocumentRef:
		return "ref"
	case []any:
		return "array"
	case map[string]any:
		return "map"
	default:
		return "string"
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

// --- Import functions ---

type importRecord struct {
	path      string
	data      map[string]any
	refFields map[string]bool // fields that are Firestore document references
}

// castValue converts a CSV string to the correct Go type based on the type label.
func castValue(raw string, typeName string) (any, error) {
	if raw == "" {
		return nil, nil
	}
	switch typeName {
	case "string":
		return raw, nil
	case "bool":
		return strconv.ParseBool(raw)
	case "int":
		return strconv.ParseInt(raw, 10, 64)
	case "float":
		return strconv.ParseFloat(raw, 64)
	case "timestamp":
		return time.Parse(time.RFC3339Nano, raw)
	case "geo":
		var obj map[string]float64
		if err := json.Unmarshal([]byte(raw), &obj); err != nil {
			return nil, fmt.Errorf("invalid geo JSON: %w", err)
		}
		lat, hasLat := obj["lat"]
		lng, hasLng := obj["lng"]
		if !hasLat || !hasLng {
			return nil, fmt.Errorf("geo JSON must have 'lat' and 'lng' keys")
		}
		return &latlng.LatLng{Latitude: lat, Longitude: lng}, nil
	case "bytes":
		return base64.StdEncoding.DecodeString(raw)
	case "ref":
		return raw, nil
	case "array":
		var arr []any
		if err := json.Unmarshal([]byte(raw), &arr); err != nil {
			return nil, fmt.Errorf("invalid array JSON: %w", err)
		}
		return arr, nil
	case "map":
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			return nil, fmt.Errorf("invalid map JSON: %w", err)
		}
		return m, nil
	default:
		return nil, fmt.Errorf("unknown type label: %q", typeName)
	}
}

// detectType applies heuristic type detection to a raw CSV string.
func detectType(raw string) (any, error) {
	if raw == "" {
		return nil, nil
	}

	// bool (case-sensitive)
	if raw == "true" {
		return true, nil
	}
	if raw == "false" {
		return false, nil
	}

	// timestamp (RFC3339)
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t, nil
	}

	// integer
	if i, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return i, nil
	}

	// float
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		// Only treat as float if it contains a decimal point or exponent,
		// otherwise it would have been caught by ParseInt above.
		if strings.ContainsAny(raw, ".eE") {
			return f, nil
		}
	}

	// JSON object
	if len(raw) >= 2 && raw[0] == '{' {
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err == nil {
			return m, nil
		}
	}

	// JSON array
	if len(raw) >= 2 && raw[0] == '[' {
		var arr []any
		if err := json.Unmarshal([]byte(raw), &arr); err == nil {
			return arr, nil
		}
	}

	// default: string
	return raw, nil
}

// parseCSVFile reads a CSV file and returns import records with typed field values.
func parseCSVFile(path string) ([]importRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("reading CSV %s: %w", path, err)
	}

	if len(rows) < 1 {
		return nil, fmt.Errorf("CSV file %s has no header row", path)
	}

	headers := rows[0]

	// Find special column indices
	pathIdx := -1
	typesIdx := -1
	for i, h := range headers {
		switch h {
		case "__path__":
			pathIdx = i
		case "__fs_types__":
			typesIdx = i
		}
	}
	if pathIdx < 0 {
		return nil, fmt.Errorf("CSV file %s is missing required __path__ column", path)
	}

	// Identify data field columns (exclude __path__ and __fs_types__)
	type fieldCol struct {
		name string
		idx  int
	}
	var dataFields []fieldCol
	for i, h := range headers {
		if i == pathIdx || i == typesIdx {
			continue
		}
		dataFields = append(dataFields, fieldCol{name: h, idx: i})
	}

	var records []importRecord
	for _, row := range rows[1:] {
		docPath := row[pathIdx]
		if docPath == "" {
			continue
		}

		// Parse type map if available
		var typeMap map[string]string
		if typesIdx >= 0 && typesIdx < len(row) && row[typesIdx] != "" {
			if err := json.Unmarshal([]byte(row[typesIdx]), &typeMap); err != nil {
				return nil, fmt.Errorf("invalid __fs_types__ JSON in row with path %q: %w", docPath, err)
			}
		}

		data := make(map[string]any, len(dataFields))
		var refFields map[string]bool
		for _, fc := range dataFields {
			raw := ""
			if fc.idx < len(row) {
				raw = row[fc.idx]
			}

			var val any
			if typeMap != nil {
				typeName, hasType := typeMap[fc.name]
				if !hasType {
					// Field not in type map — treat empty as nil, otherwise string
					if raw == "" {
						continue
					}
					val = raw
				} else {
					var castErr error
					val, castErr = castValue(raw, typeName)
					if castErr != nil {
						return nil, fmt.Errorf("casting field %q (type %q) in row %q: %w", fc.name, typeName, docPath, castErr)
					}
					if typeName == "ref" && val != nil {
						if refFields == nil {
							refFields = make(map[string]bool)
						}
						refFields[fc.name] = true
					}
				}
			} else {
				var detectErr error
				val, detectErr = detectType(raw)
				if detectErr != nil {
					return nil, fmt.Errorf("detecting type for field %q in row %q: %w", fc.name, docPath, detectErr)
				}
			}

			if val == nil {
				continue
			}
			data[fc.name] = val
		}

		records = append(records, importRecord{path: docPath, data: data, refFields: refFields})
	}

	return records, nil
}

type importConfig struct {
	project    string
	database   string
	emulator   string
	inputs     []string
	onConflict string
	dryRun     bool
}

var validConflictStrategies = map[string]bool{
	"skip": true, "overwrite": true, "merge": true, "fail": true,
}

func runImportCmd(cmd *cobra.Command, args []string) error {
	project, database, emulator, err := validateConnectionFlags(cmd)
	if err != nil {
		return err
	}

	f := cmd.Flags()
	inputs, _ := f.GetStringSlice("input")
	onConflict, _ := f.GetString("on-conflict")
	dryRun, _ := f.GetBool("dry-run")

	if !validConflictStrategies[onConflict] {
		return fmt.Errorf("invalid --on-conflict value %q: must be one of skip, overwrite, merge, fail", onConflict)
	}

	return runImport(importConfig{
		project:    project,
		database:   database,
		emulator:   emulator,
		inputs:     inputs,
		onConflict: onConflict,
		dryRun:     dryRun,
	})
}

// discoverCSVFiles resolves input paths to a list of CSV file paths.
func discoverCSVFiles(inputs []string) ([]string, error) {
	seen := make(map[string]bool)
	var files []string

	for _, input := range inputs {
		info, err := os.Stat(input)
		if err != nil {
			return nil, fmt.Errorf("cannot access %q: %w", input, err)
		}

		if !info.IsDir() {
			abs, _ := filepath.Abs(input)
			if !seen[abs] {
				seen[abs] = true
				files = append(files, input)
			}
			continue
		}

		err = filepath.WalkDir(input, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if strings.ToLower(filepath.Ext(path)) == ".csv" {
				abs, _ := filepath.Abs(path)
				if !seen[abs] {
					seen[abs] = true
					files = append(files, path)
				}
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walking directory %q: %w", input, err)
		}
	}

	return files, nil
}

type importSummary struct {
	written  int
	skipped  int
	failed   int
	dryRun   int
	total    int
}

func runImport(cfg importConfig) error {
	fmt.Fprintln(os.Stderr)

	// Step 1: Discover CSV files
	csvFiles, err := discoverCSVFiles(cfg.inputs)
	if err != nil {
		return err
	}
	if len(csvFiles) == 0 {
		return fmt.Errorf("no CSV files found in the specified inputs")
	}

	displayProject := cfg.project
	if cfg.emulator != "" {
		displayProject = fmt.Sprintf("emulator @ %s", cfg.emulator)
	}
	mode := cfg.onConflict
	if cfg.dryRun {
		mode += " (dry-run)"
	}
	printInfo("Importing to %s (database: %s, conflict: %s)", bold(displayProject), bold(cfg.database), bold(mode))
	printInfo("Found %d CSV file(s)", len(csvFiles))
	fmt.Fprintln(os.Stderr)

	// Step 2: Parse all CSV files
	var allRecords []importRecord
	for _, csvFile := range csvFiles {
		records, err := parseCSVFile(csvFile)
		if err != nil {
			return fmt.Errorf("parsing %s: %w", csvFile, err)
		}
		printOK("Parsed %q — %d record(s)", csvFile, len(records))
		allRecords = append(allRecords, records...)
	}

	if len(allRecords) == 0 {
		printInfo("No records to import.")
		return nil
	}

	// Step 3: Create Firestore client (skip for dry-run)
	ctx := context.Background()
	var client *firestore.Client
	if !cfg.dryRun {
		client, err = newFirestoreClient(ctx, cfg.project, cfg.database, cfg.emulator)
		if err != nil {
			return fmt.Errorf("failed to create Firestore client: %w", err)
		}
		defer client.Close()
	}

	// Step 4: Conflict handling and writing
	summary := importSummary{total: len(allRecords)}

	// For "fail" strategy, pre-check all documents first
	if cfg.onConflict == "fail" && !cfg.dryRun {
		var conflicts []string
		for _, rec := range allRecords {
			docRef := client.Doc(rec.path)
			_, err := docRef.Get(ctx)
			if err == nil {
				conflicts = append(conflicts, rec.path)
			}
		}
		if len(conflicts) > 0 {
			printErr("Found %d existing document(s) — aborting import:", len(conflicts))
			for _, p := range conflicts {
				fmt.Fprintf(os.Stderr, "  - %s\n", p)
			}
			return fmt.Errorf("import aborted: %d conflicting document(s)", len(conflicts))
		}
	}

	for _, rec := range allRecords {
		if cfg.dryRun {
			summary.dryRun++
			continue
		}

		// Convert ref fields from path strings to DocumentRefs
		writeData := rec.data
		if len(rec.refFields) > 0 {
			writeData = make(map[string]any, len(rec.data))
			for k, v := range rec.data {
				if rec.refFields[k] {
					if s, ok := v.(string); ok {
						writeData[k] = client.Doc(s)
						continue
					}
				}
				writeData[k] = v
			}
		}

		docRef := client.Doc(rec.path)

		switch cfg.onConflict {
		case "skip":
			_, err := docRef.Get(ctx)
			if err == nil {
				// Document exists, skip it
				fmt.Fprintf(os.Stderr, "  %s  %s (already exists, skipped)\n", faint("⊘"), rec.path)
				summary.skipped++
				continue
			}
			_, err = docRef.Set(ctx, writeData)
		case "overwrite":
			_, err = docRef.Set(ctx, writeData)
		case "merge":
			_, err = docRef.Set(ctx, writeData, firestore.MergeAll)
		case "fail":
			// Pre-check already done above, just write
			_, err = docRef.Set(ctx, writeData)
		}

		if err != nil {
			printErr("Failed to write %s: %v", rec.path, err)
			summary.failed++
			continue
		}
		summary.written++
	}

	// Step 6: Print summary
	fmt.Fprintln(os.Stderr)
	if cfg.dryRun {
		// Group by collection for dry-run report
		collections := make(map[string]int)
		for _, rec := range allRecords {
			parts := strings.Split(rec.path, "/")
			if len(parts) >= 2 {
				// Collection is everything except the last segment
				colPath := strings.Join(parts[:len(parts)-1], "/")
				collections[colPath]++
			}
		}
		printInfo("Dry-run summary:")
		for _, col := range sortedKeys(collections) {
			fmt.Fprintf(os.Stderr, "  %s: %d document(s)\n", col, collections[col])
		}
		fmt.Fprintf(os.Stderr, "\n%s Would import %d document(s) total. No changes were made.\n",
			green("✓"), summary.dryRun)
	} else {
		parts := []string{fmt.Sprintf("%d written", summary.written)}
		if summary.skipped > 0 {
			parts = append(parts, fmt.Sprintf("%d skipped", summary.skipped))
		}
		if summary.failed > 0 {
			parts = append(parts, fmt.Sprintf("%d failed", summary.failed))
		}
		fmt.Fprintf(os.Stderr, "%s Import complete: %s (total: %d)\n",
			green("✓"), strings.Join(parts, ", "), summary.total)
	}

	if summary.failed > 0 {
		return fmt.Errorf("import completed with %d error(s)", summary.failed)
	}
	return nil
}
