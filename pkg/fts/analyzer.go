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

type DescribedPipeline interface {
	Pipeline
	AnalyzerName() string
	AnalyzerVersion() uint32
	AnalyzerFingerprint() string
}

func NewAnalyzerDescriptor(name string, version uint32) AnalyzerDescriptor {
	spec := fmt.Sprintf("%s@%d", name, version)
	sum := sha256.Sum256([]byte(spec))
	return AnalyzerDescriptor{Name: name, Version: version, Fingerprint: hex.EncodeToString(sum[:])}
}

func DescribePipeline(pipeline Pipeline) (AnalyzerDescriptor, bool) {
	described, ok := pipeline.(DescribedPipeline)
	if !ok {
		return AnalyzerDescriptor{}, false
	}
	descriptor := AnalyzerDescriptor{
		Name: described.AnalyzerName(), Version: described.AnalyzerVersion(), Fingerprint: described.AnalyzerFingerprint(),
	}
	if descriptor.Name == "" || descriptor.Fingerprint == "" {
		return AnalyzerDescriptor{}, false
	}
	return descriptor, true
}
