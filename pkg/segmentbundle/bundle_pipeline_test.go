package segmentbundle

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

func TestLoadBundleRejectsLegacyPipelineMetadata(t *testing.T) {
	legacy := fts.PipelineDescriptor{
		Name: "default", Fingerprint: legacyBundleFingerprint("default", 1),
	}
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(bundleEnvelope{
		Version:   bundleVersion,
		Fields:    []bundleField{{FieldName: fts.DefaultField}},
		Pipelines: map[string]fts.PipelineDescriptor{fts.DefaultField: legacy},
	}); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if _, err := LoadBundle(&payload); err == nil || !errors.Is(err, fts.ErrPersistenceRebuildRequired) {
		t.Fatalf("LoadBundle() error = %v, want rebuild guidance", err)
	}
}

func TestLoadBundleRejectsLegacyVersion(t *testing.T) {
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(legacyBundleEnvelopeFixture{
		Version: bundleVersion - 1,
		Analyzers: map[string]legacyAnalyzerDescriptorFixture{
			fts.DefaultField: {Name: "default", Fingerprint: legacyBundleFingerprint("default", 1), FingerprintScheme: 1},
		},
	}); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if _, err := LoadBundle(&payload); err == nil || !errors.Is(err, fts.ErrPersistenceRebuildRequired) || !strings.Contains(err.Error(), "legacy version 5 uses analyzer metadata") {
		t.Fatalf("LoadBundle() error = %v, want legacy analyzer rebuild guidance", err)
	}
}

func TestLoadBundleClassifiesOlderVersionAsRebuildRequired(t *testing.T) {
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(bundleEnvelope{Version: 1}); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if _, err := LoadBundle(&payload); err == nil || !errors.Is(err, fts.ErrPersistenceRebuildRequired) {
		t.Fatalf("LoadBundle() error = %v, want ErrPersistenceRebuildRequired", err)
	}
}

func TestSaveBundleRejectsLegacyPipelineMetadata(t *testing.T) {
	legacy := fts.PipelineDescriptor{
		Name: "default", Fingerprint: legacyBundleFingerprint("default", 1),
	}
	err := SaveMultiFieldBundle(&bytes.Buffer{}, map[string]posting.Exporter{fts.DefaultField: nil}, nil, nil, nil, BundleSaveOptions{
		Pipelines: map[string]fts.PipelineDescriptor{fts.DefaultField: legacy},
	})
	if err == nil || !errors.Is(err, fts.ErrPersistenceRebuildRequired) {
		t.Fatalf("SaveMultiFieldBundle() error = %v, want rebuild guidance", err)
	}
}

type legacyAnalyzerDescriptorFixture struct {
	Name              string
	Fingerprint       string
	FingerprintScheme uint32
}

type legacyBundleEnvelopeFixture struct {
	Version   uint16
	Analyzers map[string]legacyAnalyzerDescriptorFixture
}

func legacyBundleFingerprint(name string, version uint32) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s@%d", name, version)))
	return hex.EncodeToString(sum[:])
}
