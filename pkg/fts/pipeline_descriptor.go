package fts

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const sha256FingerprintPrefix = "sha256:"

// SHA256Fingerprint returns a self-describing fingerprint for canonical
// configuration bytes owned by a pipeline or one of its components.
func SHA256Fingerprint(configuration []byte) string {
	sum := sha256.Sum256(configuration)
	return sha256FingerprintPrefix + hex.EncodeToString(sum[:])
}

// PipelineDescriptor identifies the tokenization configuration persisted into
// an index. Name is diagnostic metadata; Fingerprint defines compatibility.
type PipelineDescriptor struct {
	Name        string
	Fingerprint string
}

// NewPipelineDescriptor constructs a descriptor from a pipeline-supplied
// configuration fingerprint. Fingerprints include their digest algorithm,
// for example "sha256:<64 lowercase hexadecimal characters>".
func NewPipelineDescriptor(name, fingerprint string) (PipelineDescriptor, error) {
	descriptor := PipelineDescriptor{Name: name, Fingerprint: fingerprint}
	if err := descriptor.Validate(); err != nil {
		return PipelineDescriptor{}, err
	}
	return descriptor, nil
}

func ValidatePipelineDescriptor(descriptor PipelineDescriptor) error {
	return descriptor.Validate()
}

func (descriptor PipelineDescriptor) Validate() error {
	if !validPipelineName(descriptor.Name) {
		return fmt.Errorf("fts: invalid pipeline name %q", descriptor.Name)
	}
	if err := validateFingerprint(descriptor.Fingerprint); err != nil {
		return fmt.Errorf("fts: pipeline %q: %w", descriptor.Name, err)
	}
	return nil
}

// ValidateFingerprint verifies a self-describing configuration fingerprint.
func ValidateFingerprint(fingerprint string) error {
	if err := validateFingerprint(fingerprint); err != nil {
		return fmt.Errorf("fts: %w", err)
	}
	return nil
}

func validateFingerprint(fingerprint string) error {
	if fingerprint == "" {
		return fmt.Errorf("fingerprint is empty")
	}
	algorithm, digest, ok := strings.Cut(fingerprint, ":")
	if !ok {
		if len(fingerprint) == 64 {
			if _, err := hex.DecodeString(fingerprint); err == nil {
				return fmt.Errorf("legacy unqualified fingerprint: %w", ErrPersistenceRebuildRequired)
			}
		}
		return fmt.Errorf("fingerprint must include its digest algorithm")
	}
	if algorithm != strings.TrimSuffix(sha256FingerprintPrefix, ":") {
		return fmt.Errorf("unsupported fingerprint algorithm %q", algorithm)
	}
	if len(digest) != 64 {
		return fmt.Errorf("sha256 fingerprint must contain 64 hexadecimal characters")
	}
	if digest != strings.ToLower(digest) {
		return fmt.Errorf("fingerprint must use lowercase hexadecimal")
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return fmt.Errorf("invalid fingerprint: %w", err)
	}
	return nil
}

func validPipelineName(name string) bool {
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

// ValidatePipelineDescriptors verifies descriptor integrity and exact field compatibility.
func ValidatePipelineDescriptors(expected, actual map[string]PipelineDescriptor) error {
	for fieldName, descriptor := range expected {
		if fieldName == "" {
			return fmt.Errorf("fts: expected pipeline has an empty field name")
		}
		if err := ValidatePipelineDescriptor(descriptor); err != nil {
			return fmt.Errorf("fts: field %q: invalid expected pipeline descriptor: %w", fieldName, err)
		}
	}
	for fieldName, descriptor := range actual {
		if fieldName == "" {
			return fmt.Errorf("fts: actual pipeline has an empty field name")
		}
		if err := ValidatePipelineDescriptor(descriptor); err != nil {
			return fmt.Errorf("fts: field %q: invalid actual pipeline descriptor: %w", fieldName, err)
		}
	}

	for fieldName, want := range expected {
		got, ok := actual[fieldName]
		if !ok {
			return fmt.Errorf("fts: field %q: pipeline is missing", fieldName)
		}
		if got.Fingerprint != want.Fingerprint {
			return fmt.Errorf("fts: field %q: pipeline fingerprint mismatch: got %q (%q), want %q (%q)", fieldName, got.Fingerprint, got.Name, want.Fingerprint, want.Name)
		}
	}
	for fieldName := range actual {
		if _, ok := expected[fieldName]; !ok {
			return fmt.Errorf("fts: field %q: unexpected pipeline", fieldName)
		}
	}
	return nil
}

// ValidatePipelineFields verifies descriptor integrity and exact field coverage.
func ValidatePipelineFields(fields []string, pipelines map[string]PipelineDescriptor) error {
	if err := ValidatePipelineDescriptors(pipelines, pipelines); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(fields))
	for _, fieldName := range fields {
		if fieldName == "" {
			return fmt.Errorf("fts: pipeline field name is empty")
		}
		if _, ok := seen[fieldName]; ok {
			return fmt.Errorf("fts: duplicate pipeline field %q", fieldName)
		}
		seen[fieldName] = struct{}{}
		if _, ok := pipelines[fieldName]; !ok {
			return fmt.Errorf("fts: field %q: pipeline is missing", fieldName)
		}
	}
	for fieldName := range pipelines {
		if _, ok := seen[fieldName]; !ok {
			return fmt.Errorf("fts: field %q: unexpected pipeline", fieldName)
		}
	}
	return nil
}
