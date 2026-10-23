package textproc

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"unicode"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

const snowballVersion = "github.com/kljensen/snowball@v0.10.0"

// ComponentDescriptor is the stable identity and configuration of a tokenizer
// or filter. Component versions must change when behavior changes without a
// corresponding parameter change.
type ComponentDescriptor struct {
	Name    string
	Version uint32
	Params  map[string]string
}

func (descriptor ComponentDescriptor) Validate() error {
	if !validIdentifier(descriptor.Name) {
		return fmt.Errorf("textproc: invalid component name %q", descriptor.Name)
	}
	if descriptor.Version == 0 {
		return fmt.Errorf("textproc: component %q: version must be greater than zero", descriptor.Name)
	}
	for name := range descriptor.Params {
		if !validIdentifier(name) {
			return fmt.Errorf("textproc: component %q: invalid parameter name %q", descriptor.Name, name)
		}
	}
	return nil
}

func validIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.' {
			continue
		}
		return false
	}
	return true
}

func pipelineDescriptor(name string, tokenizer Tokenizer, filters []Filter) (fts.AnalyzerDescriptor, error) {
	if name == "" {
		return fts.AnalyzerDescriptor{}, fmt.Errorf("textproc: pipeline name is empty")
	}
	if tokenizer == nil {
		return fts.AnalyzerDescriptor{}, fmt.Errorf("textproc: pipeline %q tokenizer is nil", name)
	}

	tokenizerDescriptor := tokenizer.Descriptor()
	if err := tokenizerDescriptor.Validate(); err != nil {
		return fts.AnalyzerDescriptor{}, fmt.Errorf("textproc: pipeline %q tokenizer: %w", name, err)
	}
	filterDescriptors := make([]ComponentDescriptor, len(filters))
	for i, filter := range filters {
		if filter == nil {
			return fts.AnalyzerDescriptor{}, fmt.Errorf("textproc: pipeline %q filter %d is nil", name, i)
		}
		descriptor := filter.Descriptor()
		if err := descriptor.Validate(); err != nil {
			return fts.AnalyzerDescriptor{}, fmt.Errorf("textproc: pipeline %q filter %d: %w", name, i, err)
		}
		filterDescriptors[i] = descriptor
	}

	var canonical bytes.Buffer
	canonical.WriteString("fts-engine/textproc-pipeline")
	writeUint32(&canonical, fts.AnalyzerFingerprintSchemeV1)
	writeComponent(&canonical, tokenizerDescriptor)
	writeUint64(&canonical, uint64(len(filterDescriptors)))
	for _, descriptor := range filterDescriptors {
		writeComponent(&canonical, descriptor)
	}

	sum := sha256.Sum256(canonical.Bytes())
	fingerprint := hex.EncodeToString(sum[:])
	return fts.NewAnalyzerDescriptor(name, fingerprint, fts.AnalyzerFingerprintSchemeV1)
}

func writeComponent(buffer *bytes.Buffer, descriptor ComponentDescriptor) {
	writeString(buffer, descriptor.Name)
	writeUint32(buffer, descriptor.Version)
	keys := make([]string, 0, len(descriptor.Params))
	for key := range descriptor.Params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	writeUint64(buffer, uint64(len(keys)))
	for _, key := range keys {
		writeString(buffer, key)
		writeString(buffer, descriptor.Params[key])
	}
}

func writeString(buffer *bytes.Buffer, value string) {
	writeUint64(buffer, uint64(len(value)))
	buffer.WriteString(value)
}

func writeUint32(buffer *bytes.Buffer, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	buffer.Write(encoded[:])
}

func writeUint64(buffer *bytes.Buffer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	buffer.Write(encoded[:])
}

func component(name string, params map[string]string) ComponentDescriptor {
	return ComponentDescriptor{Name: name, Version: 1, Params: params}
}

func unicodeParams() map[string]string {
	return map[string]string{"unicode_version": unicode.Version}
}

func minLengthParams(minLength int) map[string]string {
	if minLength <= 0 {
		minLength = 1
	}
	return map[string]string{"min_length": strconv.Itoa(minLength)}
}
