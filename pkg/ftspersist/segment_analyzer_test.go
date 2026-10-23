package ftspersist

import (
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func TestLoadSegmentDataRejectsInvalidAnalyzerMetadata(t *testing.T) {
	valid, err := fts.NewAnalyzerDescriptor("default", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", fts.AnalyzerFingerprintSchemeV1)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name        string
		version     uint16
		analyzers   map[string]fts.AnalyzerDescriptor
		wantRebuild bool
	}{
		{name: "missing", version: segmentManifestVersion, analyzers: nil},
		{name: "malformed", version: segmentManifestVersion, analyzers: map[string]fts.AnalyzerDescriptor{fts.DefaultField: {Name: valid.Name, Fingerprint: "invalid", FingerprintScheme: valid.FingerprintScheme}}},
		{name: "legacy fingerprint", version: segmentManifestVersion, wantRebuild: true, analyzers: map[string]fts.AnalyzerDescriptor{fts.DefaultField: {Name: valid.Name, Fingerprint: legacyAnalyzerFingerprint(valid.Name, 1)}}},
		{name: "legacy manifest", version: segmentManifestVersion - 1, analyzers: nil, wantRebuild: true},
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
				Fields:    []segmentFieldMeta{{FieldName: fts.DefaultField, FileName: "field.segment.fidx"}},
				Analyzers: test.analyzers,
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
			if test.wantRebuild && !strings.Contains(err.Error(), "rebuild") {
				t.Fatalf("loadSegmentData() error = %v, want rebuild guidance", err)
			}
		})
	}
}

func legacyAnalyzerFingerprint(name string, version uint32) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s@%d", name, version)))
	return hex.EncodeToString(sum[:])
}
