package textproc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"unicode"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
)

const (
	snowballSource = "github.com/kljensen/snowball@v0.10.0"
)

func pipelineDescriptor(name string, tokenizer Tokenizer, filters []Filter) (fts.PipelineDescriptor, error) {
	if name == "" {
		return fts.PipelineDescriptor{}, fmt.Errorf("textproc: pipeline name is empty")
	}
	if tokenizer == nil {
		return fts.PipelineDescriptor{}, fmt.Errorf("textproc: pipeline %q tokenizer is nil", name)
	}

	tokenizerFingerprint := tokenizer.Fingerprint()
	if err := validateComponentFingerprint(tokenizerFingerprint); err != nil {
		return fts.PipelineDescriptor{}, fmt.Errorf("textproc: pipeline %q tokenizer: %w", name, err)
	}
	filterFingerprints := make([]string, len(filters))
	for i, filter := range filters {
		if filter == nil {
			return fts.PipelineDescriptor{}, fmt.Errorf("textproc: pipeline %q filter %d is nil", name, i)
		}
		fingerprint := filter.Fingerprint()
		if err := validateComponentFingerprint(fingerprint); err != nil {
			return fts.PipelineDescriptor{}, fmt.Errorf("textproc: pipeline %q filter %d: %w", name, i, err)
		}
		filterFingerprints[i] = fingerprint
	}

	var canonical bytes.Buffer
	writeString(&canonical, "fts-engine/textproc-pipeline/v1")
	writeString(&canonical, tokenizerFingerprint)
	writeUint64(&canonical, uint64(len(filterFingerprints)))
	for _, fingerprint := range filterFingerprints {
		writeString(&canonical, fingerprint)
	}

	return fts.NewPipelineDescriptor(name, fts.SHA256Fingerprint(canonical.Bytes()))
}

func componentFingerprint(name string, configuration ...string) string {
	var canonical bytes.Buffer
	writeString(&canonical, "fts-engine/textproc-component/v1")
	writeString(&canonical, name)
	writeUint64(&canonical, uint64(len(configuration)))
	for _, value := range configuration {
		writeString(&canonical, value)
	}
	return fts.SHA256Fingerprint(canonical.Bytes())
}

func validateComponentFingerprint(value string) error {
	return fts.ValidateFingerprint(value)
}

func writeString(buffer *bytes.Buffer, value string) {
	writeUint64(buffer, uint64(len(value)))
	buffer.WriteString(value)
}

func writeUint64(buffer *bytes.Buffer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	buffer.Write(encoded[:])
}

func unicodeFingerprint(name string) string {
	return componentFingerprint(name, "unicode="+unicode.Version)
}

func minLengthFingerprint(minLength int) string {
	if minLength <= 0 {
		minLength = 1
	}
	return componentFingerprint("min-length-or-numeric/v1", "min-length="+strconv.Itoa(minLength))
}
