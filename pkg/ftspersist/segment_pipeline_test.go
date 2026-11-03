package ftspersist

import (
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
)

func TestLoadSegmentDataRejectsInvalidPipelineMetadata(t *testing.T) {
	valid, err := fts.NewPipelineDescriptor("default", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name                string
		version             uint16
		pipelines           map[string]fts.PipelineDescriptor
		wantRebuildRequired bool
	}{
		{name: "missing", version: segmentManifestVersion, pipelines: nil},
		{name: "malformed", version: segmentManifestVersion, pipelines: map[string]fts.PipelineDescriptor{fts.DefaultField: {Name: valid.Name, Fingerprint: "invalid"}}},
		{name: "legacy fingerprint", version: segmentManifestVersion, wantRebuildRequired: true, pipelines: map[string]fts.PipelineDescriptor{fts.DefaultField: {Name: valid.Name, Fingerprint: legacyPipelineFingerprint(valid.Name, 1)}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			file, err := os.Create(filepath.Join(dir, segmentManifestFile))
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			manifest := segmentManifest{
				Version:   test.version,
				Fields:    []segmentFieldMeta{{FieldName: fts.DefaultField, FileName: "field.ftse.fidx"}},
				Pipelines: test.pipelines,
			}
			if err := gob.NewEncoder(file).Encode(manifest); err != nil {
				_ = file.Close()
				t.Fatalf("Encode() error = %v", err)
			}
			if err := file.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			_, err = loadSegmentData(SegmentPaths{Dir: dir}, SegmentLoadOptions{
				Access: AccessFile, Registry: fts.NewSnapshotRegistry(),
			})
			if err == nil {
				t.Fatal("loadSegmentData() error = nil")
			}
			if got := errors.Is(err, fts.ErrPersistenceRebuildRequired); got != test.wantRebuildRequired {
				t.Fatalf("errors.Is(ErrPersistenceRebuildRequired) = %t, want %t; error=%v", got, test.wantRebuildRequired, err)
			}
		})
	}
}

func TestLoadSegmentDataRejectsLegacyAnalyzerManifest(t *testing.T) {
	dir := t.TempDir()
	file, err := os.Create(filepath.Join(dir, segmentManifestFile))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	fixture := legacySegmentManifestFixture{
		Version: segmentManifestVersion - 1,
		Analyzers: map[string]legacyAnalyzerDescriptorFixture{
			fts.DefaultField: {Name: "default", Fingerprint: legacyPipelineFingerprint("default", 1), FingerprintScheme: 1},
		},
	}
	if err := gob.NewEncoder(file).Encode(fixture); err != nil {
		_ = file.Close()
		t.Fatalf("Encode() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = loadSegmentData(SegmentPaths{Dir: dir}, SegmentLoadOptions{Registry: fts.NewSnapshotRegistry()})
	if err == nil || !errors.Is(err, fts.ErrPersistenceRebuildRequired) || !strings.Contains(err.Error(), "legacy manifest version 4 uses analyzer metadata") {
		t.Fatalf("loadSegmentData() error = %v, want legacy analyzer rebuild guidance", err)
	}
}

func TestLoadSegmentDataClassifiesOlderManifestAsRebuildRequired(t *testing.T) {
	dir := t.TempDir()
	file, err := os.Create(filepath.Join(dir, segmentManifestFile))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := gob.NewEncoder(file).Encode(segmentManifest{Version: 1}); err != nil {
		_ = file.Close()
		t.Fatalf("Encode() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = loadSegmentData(SegmentPaths{Dir: dir}, SegmentLoadOptions{Registry: fts.NewSnapshotRegistry()})
	if err == nil || !errors.Is(err, fts.ErrPersistenceRebuildRequired) {
		t.Fatalf("loadSegmentData() error = %v, want ErrPersistenceRebuildRequired", err)
	}
}

type legacyAnalyzerDescriptorFixture struct {
	Name              string
	Fingerprint       string
	FingerprintScheme uint32
}

type legacySegmentManifestFixture struct {
	Version   uint16
	Analyzers map[string]legacyAnalyzerDescriptorFixture
}

func legacyPipelineFingerprint(name string, version uint32) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s@%d", name, version)))
	return hex.EncodeToString(sum[:])
}
