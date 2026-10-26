package fts

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
)

type customPipeline struct {
	descriptor PipelineDescriptor
}

func (p customPipeline) Process(text string) []string { return strings.Fields(text) }

func (p customPipeline) Descriptor() PipelineDescriptor { return p.descriptor }

var _ Pipeline = customPipeline{}

func TestNewPipelineDescriptor(t *testing.T) {
	fingerprint := testFingerprint("english configuration")
	descriptor, err := NewPipelineDescriptor("english", fingerprint)
	if err != nil {
		t.Fatalf("NewPipelineDescriptor() error = %v", err)
	}
	if descriptor.Name != "english" {
		t.Fatalf("NewPipelineDescriptor() = %+v", descriptor)
	}
	if descriptor.Fingerprint != fingerprint {
		t.Fatalf("Fingerprint = %q, want %q", descriptor.Fingerprint, fingerprint)
	}
	if err := ValidatePipelineDescriptor(descriptor); err != nil {
		t.Fatalf("ValidatePipelineDescriptor() error = %v", err)
	}
}

func TestNewPipelineDescriptorRejectsInvalidIdentity(t *testing.T) {
	tests := []struct {
		caseName    string
		name        string
		fingerprint string
	}{
		{caseName: "empty name", name: "", fingerprint: testFingerprint("config")},
		{caseName: "name whitespace", name: " english", fingerprint: testFingerprint("config")},
		{caseName: "empty fingerprint", name: "english"},
	}
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			if _, err := NewPipelineDescriptor(test.name, test.fingerprint); err == nil {
				t.Fatal("NewPipelineDescriptor() error = nil")
			}
		})
	}
}

func TestValidatePipelineDescriptorRejectsFingerprint(t *testing.T) {
	tests := []struct {
		name                string
		fingerprint         string
		wantRebuildRequired bool
	}{
		{name: "unsupported algorithm", fingerprint: "md5:" + strings.Repeat("a", 32)},
		{name: "short digest", fingerprint: "sha256:abc"},
		{name: "invalid hexadecimal", fingerprint: "sha256:" + strings.Repeat("z", 64)},
		{name: "uppercase hexadecimal", fingerprint: "sha256:" + strings.Repeat("A", 64)},
		{name: "missing algorithm", fingerprint: "invalid"},
		{name: "legacy", fingerprint: strings.Repeat("a", 64), wantRebuildRequired: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			descriptor := PipelineDescriptor{Name: "english", Fingerprint: test.fingerprint}
			err := descriptor.Validate()
			if err == nil {
				t.Fatal("Validate() error = nil")
			}
			if got := errors.Is(err, ErrPersistenceRebuildRequired); got != test.wantRebuildRequired {
				t.Fatalf("errors.Is(ErrPersistenceRebuildRequired) = %t, want %t; error=%v", got, test.wantRebuildRequired, err)
			}
		})
	}
}

func TestValidatePipelineDescriptors(t *testing.T) {
	english, _ := NewPipelineDescriptor("english", testFingerprint("english"))
	englishOther, _ := NewPipelineDescriptor("english", testFingerprint("english-other"))
	russianSame, _ := NewPipelineDescriptor("russian", english.Fingerprint)

	tests := []struct {
		name     string
		expected map[string]PipelineDescriptor
		actual   map[string]PipelineDescriptor
		wantOK   bool
	}{
		{name: "matching", expected: map[string]PipelineDescriptor{"title": english}, actual: map[string]PipelineDescriptor{"title": english}, wantOK: true},
		{name: "different name", expected: map[string]PipelineDescriptor{"title": english}, actual: map[string]PipelineDescriptor{"title": russianSame}, wantOK: true},
		{name: "missing field", expected: map[string]PipelineDescriptor{"title": english}, actual: map[string]PipelineDescriptor{}},
		{name: "unexpected field", expected: map[string]PipelineDescriptor{}, actual: map[string]PipelineDescriptor{"title": english}},
		{name: "different fingerprint", expected: map[string]PipelineDescriptor{"title": english}, actual: map[string]PipelineDescriptor{"title": englishOther}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidatePipelineDescriptors(test.expected, test.actual)
			if test.wantOK && err != nil {
				t.Fatalf("ValidatePipelineDescriptors() error = %v", err)
			}
			if !test.wantOK && err == nil {
				t.Fatal("ValidatePipelineDescriptors() error = nil")
			}
		})
	}
}

func TestCustomPipelineSatisfiesPipeline(t *testing.T) {
	descriptor, _ := NewPipelineDescriptor("custom", testFingerprint("custom"))
	pipeline := customPipeline{descriptor: descriptor}
	if got := pipeline.Process("alpha beta"); len(got) != 2 {
		t.Fatalf("Process() = %v", got)
	}
	if pipeline.Descriptor() != descriptor {
		t.Fatalf("Descriptor() = %+v, want %+v", pipeline.Descriptor(), descriptor)
	}
}

func TestPipelineDescriptorsIncludesEmptyIndex(t *testing.T) {
	descriptor, _ := NewPipelineDescriptor("custom", testFingerprint("custom"))
	svc := New(newMemoryIndex(), WordKeys, WithPipeline(customPipeline{descriptor: descriptor}))

	descriptors, err := svc.PipelineDescriptors()
	if err != nil {
		t.Fatalf("PipelineDescriptors() error = %v", err)
	}
	if got := descriptors[DefaultField]; got != descriptor {
		t.Fatalf("PipelineDescriptors()[%q] = %+v, want %+v", DefaultField, got, descriptor)
	}
}

func TestPipelineDescriptorsRejectConfiguredPipelineMismatch(t *testing.T) {
	other, _ := NewPipelineDescriptor("other", testFingerprint("other"))
	svc := New(newMemoryIndex(), WordKeys, WithPipelineDescriptors(map[string]PipelineDescriptor{
		DefaultField: other,
	}))
	if _, err := svc.PipelineDescriptors(); err == nil {
		t.Fatal("PipelineDescriptors() error = nil")
	}
}

func testFingerprint(configuration string) string {
	sum := sha256.Sum256([]byte(configuration))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func legacyTestFingerprint(name string, version uint32) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s@%d", name, version)))
	return hex.EncodeToString(sum[:])
}
