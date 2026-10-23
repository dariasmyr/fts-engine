package segmentbundle

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/segment"
)

func TestLoadBundleRejectsLegacyAnalyzerMetadata(t *testing.T) {
	legacy := fts.AnalyzerDescriptor{
		Name: "default", Fingerprint: legacyBundleFingerprint("default", 1),
	}
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(bundleEnvelope{
		Version:   bundleVersion,
		Fields:    []bundleField{{FieldName: fts.DefaultField}},
		Analyzers: map[string]fts.AnalyzerDescriptor{fts.DefaultField: legacy},
	}); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if _, err := LoadBundle(&payload); err == nil || !strings.Contains(err.Error(), "rebuild") {
		t.Fatalf("LoadBundle() error = %v, want rebuild guidance", err)
	}
}

func TestLoadBundleRejectsLegacyVersion(t *testing.T) {
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(bundleEnvelope{Version: bundleVersion - 1}); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if _, err := LoadBundle(&payload); err == nil || !strings.Contains(err.Error(), "rebuild") {
		t.Fatalf("LoadBundle() error = %v, want rebuild guidance", err)
	}
}

func TestSaveBundleRejectsLegacyAnalyzerMetadata(t *testing.T) {
	legacy := fts.AnalyzerDescriptor{
		Name: "default", Fingerprint: legacyBundleFingerprint("default", 1),
	}
	err := SaveMultiFieldBundle(&bytes.Buffer{}, map[string]segment.Source{fts.DefaultField: nil}, nil, nil, nil, BundleSaveOptions{
		Analyzers: map[string]fts.AnalyzerDescriptor{fts.DefaultField: legacy},
	})
	if err == nil || !strings.Contains(err.Error(), "rebuild") {
		t.Fatalf("SaveMultiFieldBundle() error = %v, want rebuild guidance", err)
	}
}

func legacyBundleFingerprint(name string, version uint32) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s@%d", name, version)))
	return hex.EncodeToString(sum[:])
}
