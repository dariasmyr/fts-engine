package ftspersist

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func TestLoadSegmentDataRejectsInvalidAnalyzerMetadata(t *testing.T) {
	valid, err := fts.NewAnalyzerDescriptor("default", 1)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		analyzers map[string]fts.AnalyzerDescriptor
	}{
		{name: "missing", analyzers: nil},
		{name: "malformed", analyzers: map[string]fts.AnalyzerDescriptor{fts.DefaultField: {Name: valid.Name, Version: valid.Version, Fingerprint: "invalid"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			file, err := os.Create(filepath.Join(dir, segmentManifestFile))
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			manifest := segmentManifest{
				Version:   segmentManifestVersion,
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
			if _, err := loadSegmentData(SegmentPaths{Dir: dir}, SegmentLoadOptions{
				Access: AccessFile, Registry: fts.NewSnapshotRegistry(),
			}); err == nil {
				t.Fatal("loadSegmentData() error = nil")
			}
		})
	}
}
