package fts

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
)

type customPipeline struct {
	descriptor AnalyzerDescriptor
}

func (p customPipeline) Process(text string) []string { return strings.Fields(text) }

func (p customPipeline) Descriptor() AnalyzerDescriptor { return p.descriptor }

var _ Pipeline = customPipeline{}

func TestNewAnalyzerDescriptor(t *testing.T) {
	fingerprint := testFingerprint("english configuration")
	descriptor, err := NewAnalyzerDescriptor("english", fingerprint, AnalyzerFingerprintSchemeV1)
	if err != nil {
		t.Fatalf("NewAnalyzerDescriptor() error = %v", err)
	}
	if descriptor.Name != "english" || descriptor.FingerprintScheme != AnalyzerFingerprintSchemeV1 {
		t.Fatalf("NewAnalyzerDescriptor() = %+v", descriptor)
	}
	if descriptor.Fingerprint != fingerprint {
		t.Fatalf("Fingerprint = %q, want %q", descriptor.Fingerprint, fingerprint)
	}
	if err := ValidateAnalyzerDescriptor(descriptor); err != nil {
		t.Fatalf("ValidateAnalyzerDescriptor() error = %v", err)
	}
}

func TestNewAnalyzerDescriptorRejectsInvalidIdentity(t *testing.T) {
	tests := []struct {
		caseName    string
		name        string
		scheme      uint32
		fingerprint string
	}{
		{caseName: "empty name", name: "", scheme: AnalyzerFingerprintSchemeV1, fingerprint: testFingerprint("config")},
		{caseName: "name whitespace", name: " english", scheme: AnalyzerFingerprintSchemeV1, fingerprint: testFingerprint("config")},
		{caseName: "zero scheme", name: "english", fingerprint: testFingerprint("config")},
		{caseName: "empty fingerprint", name: "english", scheme: AnalyzerFingerprintSchemeV1},
	}
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			if _, err := NewAnalyzerDescriptor(test.name, test.fingerprint, test.scheme); err == nil {
				t.Fatal("NewAnalyzerDescriptor() error = nil")
			}
		})
	}
}

func TestValidateAnalyzerDescriptorRejectsFingerprint(t *testing.T) {
	tests := []struct {
		name        string
		scheme      uint32
		fingerprint string
	}{
		{name: "unsupported scheme", scheme: 99, fingerprint: testFingerprint("config")},
		{name: "short digest", scheme: AnalyzerFingerprintSchemeV1, fingerprint: "abc"},
		{name: "invalid hexadecimal", scheme: AnalyzerFingerprintSchemeV1, fingerprint: strings.Repeat("z", 64)},
		{name: "uppercase hexadecimal", scheme: AnalyzerFingerprintSchemeV1, fingerprint: strings.Repeat("A", 64)},
		{name: "legacy", fingerprint: legacyTestFingerprint("english", 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			descriptor := AnalyzerDescriptor{Name: "english", Fingerprint: test.fingerprint, FingerprintScheme: test.scheme}
			err := descriptor.Validate()
			if err == nil {
				t.Fatal("Validate() error = nil")
			}
			if test.name == "legacy" && !strings.Contains(err.Error(), "rebuild") {
				t.Fatalf("Validate() error = %v, want rebuild guidance", err)
			}
		})
	}
}

func TestValidateAnalyzers(t *testing.T) {
	english, _ := NewAnalyzerDescriptor("english", testFingerprint("english"), AnalyzerFingerprintSchemeV1)
	englishOther, _ := NewAnalyzerDescriptor("english", testFingerprint("english-other"), AnalyzerFingerprintSchemeV1)
	russianSame, _ := NewAnalyzerDescriptor("russian", english.Fingerprint, AnalyzerFingerprintSchemeV1)
	otherScheme := AnalyzerDescriptor{Name: "english", Fingerprint: english.Fingerprint, FingerprintScheme: 2}

	tests := []struct {
		name     string
		expected map[string]AnalyzerDescriptor
		actual   map[string]AnalyzerDescriptor
		wantOK   bool
	}{
		{name: "matching", expected: map[string]AnalyzerDescriptor{"title": english}, actual: map[string]AnalyzerDescriptor{"title": english}, wantOK: true},
		{name: "different name", expected: map[string]AnalyzerDescriptor{"title": english}, actual: map[string]AnalyzerDescriptor{"title": russianSame}, wantOK: true},
		{name: "missing field", expected: map[string]AnalyzerDescriptor{"title": english}, actual: map[string]AnalyzerDescriptor{}},
		{name: "unexpected field", expected: map[string]AnalyzerDescriptor{}, actual: map[string]AnalyzerDescriptor{"title": english}},
		{name: "different scheme", expected: map[string]AnalyzerDescriptor{"title": english}, actual: map[string]AnalyzerDescriptor{"title": otherScheme}},
		{name: "different fingerprint", expected: map[string]AnalyzerDescriptor{"title": english}, actual: map[string]AnalyzerDescriptor{"title": englishOther}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateAnalyzers(test.expected, test.actual)
			if test.wantOK && err != nil {
				t.Fatalf("ValidateAnalyzers() error = %v", err)
			}
			if !test.wantOK && err == nil {
				t.Fatal("ValidateAnalyzers() error = nil")
			}
		})
	}
}

func TestCustomPipelineSatisfiesPipeline(t *testing.T) {
	descriptor, _ := NewAnalyzerDescriptor("custom", testFingerprint("custom"), AnalyzerFingerprintSchemeV1)
	pipeline := customPipeline{descriptor: descriptor}
	if got := pipeline.Process("alpha beta"); len(got) != 2 {
		t.Fatalf("Process() = %v", got)
	}
	if pipeline.Descriptor() != descriptor {
		t.Fatalf("Descriptor() = %+v, want %+v", pipeline.Descriptor(), descriptor)
	}
}

func TestAnalyzerDescriptorsRejectConfiguredPipelineMismatch(t *testing.T) {
	other, _ := NewAnalyzerDescriptor("other", testFingerprint("other"), AnalyzerFingerprintSchemeV1)
	svc := New(newMemoryIndex(), WordKeys, WithAnalyzerDescriptors(map[string]AnalyzerDescriptor{
		DefaultField: other,
	}))
	if _, err := svc.AnalyzerDescriptors(); err == nil {
		t.Fatal("AnalyzerDescriptors() error = nil")
	}
}

func testFingerprint(configuration string) string {
	sum := sha256.Sum256([]byte(configuration))
	return hex.EncodeToString(sum[:])
}

func legacyTestFingerprint(name string, version uint32) string {
	return testFingerprint(fmt.Sprintf("%s@%d", name, version))
}
