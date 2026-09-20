package semanticpersist

import (
	"context"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
)

func TestStandaloneSegmentRoundTrip(t *testing.T) {
	snapshot, _, wantDocuments := persistenceFixture(t, true)
	root := t.TempDir()
	path := SegmentPaths{Dir: root + "/segment"}
	if err := SaveSegment(context.Background(), path, snapshot, Options{}); err != nil {
		t.Fatal(err)
	}
	loaded, err := OpenSegment(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := loaded.Snapshot.SearchDocuments(context.Background(), zeroQueryEncoder(), semantic.Document{ID: "query"}, len(wantDocuments.Hits))
	if err != nil {
		t.Fatal(err)
	}
	if !equalDocumentHits(got.Hits, wantDocuments.Hits) {
		t.Fatalf("standalone result = %+v, want %+v", got.Hits, wantDocuments.Hits)
	}
	if _, err := OpenSegment(SegmentPaths{Dir: path.Dir + "/missing"}, Limits{}); err == nil {
		t.Fatal("missing standalone segment accepted")
	}
}
