package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// validFakerTypes enumerates the supported faker type strings.
var validFakerTypes = map[string]bool{
	"firstName":   true,
	"lastName":    true,
	"email":       true,
	"phone":       true,
	"address":     true,
	"companyName": true,
	"uuid":        true,
}

// sanitizeConfig holds the field-name → faker-type mapping.
type sanitizeConfig struct {
	Fields map[string]string `yaml:"fields"`
}

// parseSanitizeConfig parses a sanitization config from either a YAML file path
// (if raw ends with .yaml or .yml) or an inline comma-separated key=type string.
func parseSanitizeConfig(raw string) (sanitizeConfig, error) {
	var cfg sanitizeConfig

	if strings.HasSuffix(raw, ".yaml") || strings.HasSuffix(raw, ".yml") {
		data, err := os.ReadFile(raw)
		if err != nil {
			return cfg, fmt.Errorf("reading sanitize config file: %w", err)
		}
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return cfg, fmt.Errorf("parsing sanitize config YAML: %w", err)
		}
	} else {
		cfg.Fields = make(map[string]string)
		pairs := strings.Split(raw, ",")
		for _, pair := range pairs {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				continue
			}
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
				return cfg, fmt.Errorf("malformed sanitize config pair: %q (expected key=type)", pair)
			}
			cfg.Fields[parts[0]] = parts[1]
		}
	}

	if len(cfg.Fields) == 0 {
		return cfg, fmt.Errorf("sanitize config has no fields")
	}

	for field, fakerType := range cfg.Fields {
		if !validFakerTypes[fakerType] {
			supported := make([]string, 0, len(validFakerTypes))
			for k := range validFakerTypes {
				supported = append(supported, k)
			}
			sort.Strings(supported)
			return cfg, fmt.Errorf("unknown faker type %q for field %q; supported types: %s",
				fakerType, field, strings.Join(supported, ", "))
		}
	}

	return cfg, nil
}

// sanitizer replaces field values with fake data.
type sanitizer struct {
	fields map[string]string // field name → faker type
	faker  *gofakeit.Faker
}

// newSanitizer creates a sanitizer. seed=0 uses crypto/rand (non-deterministic);
// non-zero seed produces deterministic output.
func newSanitizer(cfg sanitizeConfig, seed int64) *sanitizer {
	var f *gofakeit.Faker
	if seed == 0 {
		f = gofakeit.New(0)
	} else {
		f = gofakeit.New(uint64(seed))
	}
	return &sanitizer{
		fields: cfg.Fields,
		faker:  f,
	}
}

// generate produces a fake string value for the given faker type.
func (s *sanitizer) generate(fakerType string) string {
	switch fakerType {
	case "firstName":
		return s.faker.FirstName()
	case "lastName":
		return s.faker.LastName()
	case "email":
		return s.faker.Email()
	case "phone":
		return s.faker.Phone()
	case "address":
		return s.faker.Address().Address
	case "companyName":
		return s.faker.Company()
	case "uuid":
		return s.faker.UUID()
	default:
		return ""
	}
}

// sanitizeRecord mutates data in place, replacing string values in fields
// that match the config. It recurses into nested maps and arrays of maps.
// Keys are processed in sorted order to ensure deterministic output with seeded fakers.
func (s *sanitizer) sanitizeRecord(data map[string]any) {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		val := data[key]
		if fakerType, ok := s.fields[key]; ok {
			if _, isStr := val.(string); isStr {
				data[key] = s.generate(fakerType)
			}
			continue
		}
		switch v := val.(type) {
		case map[string]any:
			s.sanitizeRecord(v)
		case []any:
			for _, elem := range v {
				if m, ok := elem.(map[string]any); ok {
					s.sanitizeRecord(m)
				}
			}
		}
	}
}

// runSanitizeCmd is the cobra RunE handler for the sanitize subcommand.
func runSanitizeCmd(cmd *cobra.Command, args []string) error {
	f := cmd.Flags()
	configFlag, _ := f.GetString("config")
	input, _ := f.GetString("input")
	output, _ := f.GetString("output")
	seed, _ := f.GetInt64("seed")

	cfg, err := parseSanitizeConfig(configFlag)
	if err != nil {
		return fmt.Errorf("invalid --config: %w", err)
	}

	return runSanitize(cfg, input, output, seed)
}

// runSanitize sanitizes CSV files from inputPath, writing results to outputDir.
func runSanitize(cfg sanitizeConfig, inputPath, outputDir string, seed int64) error {
	fmt.Fprintln(os.Stderr)

	san := newSanitizer(cfg, seed)

	csvFiles, err := discoverCSVFiles([]string{inputPath})
	if err != nil {
		return fmt.Errorf("discovering CSV files: %w", err)
	}
	if len(csvFiles) == 0 {
		printInfo("No CSV files found in %q", inputPath)
		return nil
	}

	printInfo("Found %d CSV file(s) to sanitize", len(csvFiles))

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}

	// Determine the base path for computing relative paths.
	basePath := inputPath
	info, err := os.Stat(inputPath)
	if err != nil {
		return fmt.Errorf("cannot stat input path: %w", err)
	}
	if !info.IsDir() {
		basePath = filepath.Dir(inputPath)
	}

	totalRows := 0
	for _, csvFile := range csvFiles {
		rows, err := sanitizeCSVFile(san, csvFile, basePath, outputDir)
		if err != nil {
			printErr("Failed to sanitize %q: %v", csvFile, err)
			return err
		}
		totalRows += rows
		printOK("Sanitized %q — %d rows", csvFile, rows)
	}

	fmt.Fprintf(os.Stderr, "\n%s Sanitized %d file(s), %d row(s) total.\n",
		green("✓"), len(csvFiles), totalRows)
	return nil
}

// sanitizeCSVFile reads a CSV file, replaces values in matched columns, and writes
// the result to outputDir preserving the relative path from basePath.
func sanitizeCSVFile(san *sanitizer, csvFile, basePath, outputDir string) (int, error) {
	inFile, err := os.Open(csvFile)
	if err != nil {
		return 0, fmt.Errorf("opening %q: %w", csvFile, err)
	}
	defer inFile.Close()

	reader := csv.NewReader(inFile)
	allRows, err := reader.ReadAll()
	if err != nil {
		return 0, fmt.Errorf("reading %q: %w", csvFile, err)
	}
	if len(allRows) < 1 {
		return 0, nil // empty file
	}

	headers := allRows[0]

	// Build column index → faker type mapping, skipping special columns.
	colMap := make(map[int]string) // col index → faker type
	for i, header := range headers {
		if header == "__path__" || header == "__fs_types__" {
			continue
		}
		if fakerType, ok := san.fields[header]; ok {
			colMap[i] = fakerType
		}
	}

	// Sort column indices to ensure deterministic RNG consumption order.
	sortedCols := make([]int, 0, len(colMap))
	for idx := range colMap {
		sortedCols = append(sortedCols, idx)
	}
	sort.Ints(sortedCols)

	// Replace matched cells in data rows.
	dataRows := allRows[1:]
	for _, row := range dataRows {
		for _, colIdx := range sortedCols {
			if colIdx < len(row) {
				row[colIdx] = san.generate(colMap[colIdx])
			}
		}
	}

	// Compute output path preserving relative structure.
	relPath, err := filepath.Rel(basePath, csvFile)
	if err != nil {
		relPath = filepath.Base(csvFile)
	}
	outPath := filepath.Join(outputDir, relPath)

	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		return 0, fmt.Errorf("creating output subdirectory: %w", err)
	}

	outFile, err := os.Create(outPath)
	if err != nil {
		return 0, fmt.Errorf("creating %q: %w", outPath, err)
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)
	if err := writer.WriteAll(allRows); err != nil {
		return 0, fmt.Errorf("writing %q: %w", outPath, err)
	}

	return len(dataRows), nil
}
