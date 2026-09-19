package semanticpersist

import (
	"context"
	"testing"
)

func TestStandaloneSegmentRoundTrip(t *testing.T) {
	snapshot, wantChunks, _ := persistenceFixture(t, true)
	root := t.TempDir()
	path := SegmentPaths{Dir: root + "/segment"}
	if err := SaveSegment(context.Background(), path, snapshot, Options{}); err != nil {
		t.Fatal(err)
	}
	loaded, err := OpenSegment(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := loaded.Snapshot.SearchChunks(context.Background(), []float32{0, 0}, len(wantChunks.Hits))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Hits) != len(wantChunks.Hits) || got.Hits[0] != wantChunks.Hits[0] {
		t.Fatalf("standalone result = %+v, want %+v", got.Hits, wantChunks.Hits)
	}
	if _, err := OpenSegment(SegmentPaths{Dir: path.Dir + "/missing"}, Limits{}); err == nil {
		t.Fatal("missing standalone segment accepted")
	}
}
