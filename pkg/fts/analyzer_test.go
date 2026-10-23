package fts

import (
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
	descriptor, err := NewAnalyzerDescriptor("english", 1)
	if err != nil {
		t.Fatalf("NewAnalyzerDescriptor() error = %v", err)
	}
	if descriptor.Name != "english" || descriptor.Version != 1 {
		t.Fatalf("NewAnalyzerDescriptor() = %+v", descriptor)
	}
	const wantFingerprint = "128b3d22b15f35e747507c82b86ef2eeaceae8c691ddb25aca3fe5ad88aebcd5"
	if descriptor.Fingerprint != wantFingerprint {
		t.Fatalf("Fingerprint = %q, want %q", descriptor.Fingerprint, wantFingerprint)
	}
	if err := ValidateAnalyzerDescriptor(descriptor); err != nil {
		t.Fatalf("ValidateAnalyzerDescriptor() error = %v", err)
	}
}

func TestNewAnalyzerDescriptorRejectsInvalidIdentity(t *testing.T) {
	tests := []struct {
		caseName string
		name     string
		version  uint32
	}{
		{caseName: "empty name", name: "", version: 1},
		{caseName: "zero version", name: "english", version: 0},
	}
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			if _, err := NewAnalyzerDescriptor(test.name, test.version); err == nil {
				t.Fatal("NewAnalyzerDescriptor() error = nil")
			}
		})
	}
}

func TestValidateAnalyzerDescriptorRejectsFingerprint(t *testing.T) {
	descriptor, err := NewAnalyzerDescriptor("english", 1)
	if err != nil {
		t.Fatal(err)
	}
	descriptor.Fingerprint = "not-canonical"
	if err := ValidateAnalyzerDescriptor(descriptor); err == nil {
		t.Fatal("ValidateAnalyzerDescriptor() error = nil")
	}
}

func TestValidateAnalyzers(t *testing.T) {
	englishV1, _ := NewAnalyzerDescriptor("english", 1)
	englishV2, _ := NewAnalyzerDescriptor("english", 2)
	russianV1, _ := NewAnalyzerDescriptor("russian", 1)

	tests := []struct {
		name     string
		expected map[string]AnalyzerDescriptor
		actual   map[string]AnalyzerDescriptor
	}{
		{name: "matching", expected: map[string]AnalyzerDescriptor{"title": englishV1}, actual: map[string]AnalyzerDescriptor{"title": englishV1}},
		{name: "missing field", expected: map[string]AnalyzerDescriptor{"title": englishV1}, actual: map[string]AnalyzerDescriptor{}},
		{name: "unexpected field", expected: map[string]AnalyzerDescriptor{}, actual: map[string]AnalyzerDescriptor{"title": englishV1}},
		{name: "different name", expected: map[string]AnalyzerDescriptor{"title": englishV1}, actual: map[string]AnalyzerDescriptor{"title": russianV1}},
		{name: "different version", expected: map[string]AnalyzerDescriptor{"title": englishV1}, actual: map[string]AnalyzerDescriptor{"title": englishV2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateAnalyzers(test.expected, test.actual)
			if test.name == "matching" && err != nil {
				t.Fatalf("ValidateAnalyzers() error = %v", err)
			}
			if test.name != "matching" && err == nil {
				t.Fatal("ValidateAnalyzers() error = nil")
			}
		})
	}
}

func TestCustomPipelineSatisfiesPipeline(t *testing.T) {
	descriptor, _ := NewAnalyzerDescriptor("custom", 1)
	pipeline := customPipeline{descriptor: descriptor}
	if got := pipeline.Process("alpha beta"); len(got) != 2 {
		t.Fatalf("Process() = %v", got)
	}
	if pipeline.Descriptor() != descriptor {
		t.Fatalf("Descriptor() = %+v, want %+v", pipeline.Descriptor(), descriptor)
	}
}

func TestAnalyzerDescriptorsRejectConfiguredPipelineMismatch(t *testing.T) {
	other, _ := NewAnalyzerDescriptor("other", 1)
	svc := New(newMemoryIndex(), WordKeys, WithAnalyzerDescriptors(map[string]AnalyzerDescriptor{
		DefaultField: other,
	}))
	if _, err := svc.AnalyzerDescriptors(); err == nil {
		t.Fatal("AnalyzerDescriptors() error = nil")
	}
}
