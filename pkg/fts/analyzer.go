package fts

import (
	"encoding/hex"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const AnalyzerFingerprintSchemeV1 uint32 = 1

// AnalyzerDescriptor identifies the tokenization compatibility contract
// persisted into an index. Fingerprint is supplied by the pipeline and must
// identify its complete declared configuration.
type AnalyzerDescriptor struct {
	Name              string
	Fingerprint       string
	FingerprintScheme uint32
}

// NewAnalyzerDescriptor constructs a descriptor from a pipeline-supplied
// configuration fingerprint and scheme. Name is diagnostic metadata only.
func NewAnalyzerDescriptor(name, fingerprint string, fingerprintScheme uint32) (AnalyzerDescriptor, error) {
	descriptor := AnalyzerDescriptor{Name: name, Fingerprint: fingerprint, FingerprintScheme: fingerprintScheme}
	if err := descriptor.Validate(); err != nil {
		return AnalyzerDescriptor{}, err
	}
	return descriptor, nil
}

func ValidateAnalyzerDescriptor(descriptor AnalyzerDescriptor) error {
	return descriptor.Validate()
}

func (descriptor AnalyzerDescriptor) Validate() error {
	if !validAnalyzerName(descriptor.Name) {
		return fmt.Errorf("fts: invalid analyzer name %q", descriptor.Name)
	}
	if descriptor.Fingerprint == "" {
		return fmt.Errorf("fts: analyzer %q: fingerprint is empty", descriptor.Name)
	}
	if descriptor.FingerprintScheme == 0 {
		return fmt.Errorf("fts: analyzer %q uses a legacy name/version fingerprint; rebuild persisted data", descriptor.Name)
	}
	if descriptor.FingerprintScheme != AnalyzerFingerprintSchemeV1 {
		return fmt.Errorf("fts: analyzer %q: unsupported fingerprint scheme %d", descriptor.Name, descriptor.FingerprintScheme)
	}
	if len(descriptor.Fingerprint) != 64 {
		return fmt.Errorf("fts: analyzer %q: fingerprint must contain 64 hexadecimal characters", descriptor.Name)
	}
	if descriptor.Fingerprint != strings.ToLower(descriptor.Fingerprint) {
		return fmt.Errorf("fts: analyzer %q: fingerprint must use lowercase hexadecimal", descriptor.Name)
	}
	if _, err := hex.DecodeString(descriptor.Fingerprint); err != nil {
		return fmt.Errorf("fts: analyzer %q: invalid fingerprint: %w", descriptor.Name, err)
	}
	return nil
}

func validAnalyzerName(name string) bool {
	if name == "" || !utf8.ValidString(name) || strings.TrimSpace(name) != name {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// ValidateAnalyzers verifies descriptor integrity and exact field compatibility.
func ValidateAnalyzers(expected, actual map[string]AnalyzerDescriptor) error {
	for fieldName, descriptor := range expected {
		if fieldName == "" {
			return fmt.Errorf("fts: expected analyzer has an empty field name")
		}
		if err := ValidateAnalyzerDescriptor(descriptor); err != nil {
			return fmt.Errorf("fts: field %q: invalid expected analyzer descriptor: %w", fieldName, err)
		}
	}
	for fieldName, descriptor := range actual {
		if fieldName == "" {
			return fmt.Errorf("fts: actual analyzer has an empty field name")
		}
		if err := ValidateAnalyzerDescriptor(descriptor); err != nil {
			return fmt.Errorf("fts: field %q: invalid actual analyzer descriptor: %w", fieldName, err)
		}
	}

	for fieldName, want := range expected {
		got, ok := actual[fieldName]
		if !ok {
			return fmt.Errorf("fts: field %q: analyzer is missing", fieldName)
		}
		if got.FingerprintScheme != want.FingerprintScheme {
			return fmt.Errorf("fts: field %q: analyzer fingerprint scheme mismatch: got %d (%q), want %d (%q)", fieldName, got.FingerprintScheme, got.Name, want.FingerprintScheme, want.Name)
		}
		if got.Fingerprint != want.Fingerprint {
			return fmt.Errorf("fts: field %q: analyzer fingerprint mismatch: got %q (%q), want %q (%q)", fieldName, got.Fingerprint, got.Name, want.Fingerprint, want.Name)
		}
	}
	for fieldName := range actual {
		if _, ok := expected[fieldName]; !ok {
			return fmt.Errorf("fts: field %q: unexpected analyzer", fieldName)
		}
	}
	return nil
}

// ValidateAnalyzerFields verifies descriptor integrity and exact field coverage.
func ValidateAnalyzerFields(fields []string, analyzers map[string]AnalyzerDescriptor) error {
	if err := ValidateAnalyzers(analyzers, analyzers); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(fields))
	for _, fieldName := range fields {
		if fieldName == "" {
			return fmt.Errorf("fts: analyzer field name is empty")
		}
		if _, ok := seen[fieldName]; ok {
			return fmt.Errorf("fts: duplicate analyzer field %q", fieldName)
		}
		seen[fieldName] = struct{}{}
		if _, ok := analyzers[fieldName]; !ok {
			return fmt.Errorf("fts: field %q: analyzer is missing", fieldName)
		}
	}
	for fieldName := range analyzers {
		if _, ok := seen[fieldName]; !ok {
			return fmt.Errorf("fts: field %q: unexpected analyzer", fieldName)
		}
	}
	return nil
}
