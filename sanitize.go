package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/brianvoe/gofakeit/v7"
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
