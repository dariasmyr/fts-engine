package fts

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// AnalyzerDescriptor identifies the tokenization compatibility contract
// persisted into an index. Fingerprint is derived from Name and Version; it is
// not a hash of runtime pipeline output. Version must change whenever
// identical input can produce different tokens.
type AnalyzerDescriptor struct {
	Name        string
	Version     uint32
	Fingerprint string
}

func NewAnalyzerDescriptor(name string, version uint32) (AnalyzerDescriptor, error) {
	if name == "" {
		return AnalyzerDescriptor{}, fmt.Errorf("fts: analyzer name is empty")
	}
	if version == 0 {
		return AnalyzerDescriptor{}, fmt.Errorf("fts: analyzer %q: version must be greater than zero", name)
	}

	spec := fmt.Sprintf("%s@%d", name, version)
	sum := sha256.Sum256([]byte(spec))
	return AnalyzerDescriptor{Name: name, Version: version, Fingerprint: hex.EncodeToString(sum[:])}, nil
}

func ValidateAnalyzerDescriptor(descriptor AnalyzerDescriptor) error {
	canonical, err := NewAnalyzerDescriptor(descriptor.Name, descriptor.Version)
	if err != nil {
		return err
	}
	if descriptor.Fingerprint == "" {
		return fmt.Errorf("fts: analyzer %q version %d: fingerprint is empty", descriptor.Name, descriptor.Version)
	}
	if descriptor.Fingerprint != canonical.Fingerprint {
		return fmt.Errorf("fts: analyzer %q version %d: fingerprint %q does not match canonical fingerprint %q", descriptor.Name, descriptor.Version, descriptor.Fingerprint, canonical.Fingerprint)
	}
	return nil
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
		if got.Name != want.Name {
			return fmt.Errorf("fts: field %q: analyzer name mismatch: got %q, want %q", fieldName, got.Name, want.Name)
		}
		if got.Version != want.Version {
			return fmt.Errorf("fts: field %q: analyzer version mismatch: got %d, want %d", fieldName, got.Version, want.Version)
		}
		if got.Fingerprint != want.Fingerprint {
			return fmt.Errorf("fts: field %q: analyzer fingerprint mismatch: got %q, want %q", fieldName, got.Fingerprint, want.Fingerprint)
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
