package semanticpersist

import (
	"context"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
)

func TestStandaloneSegmentRoundTrip(t *testing.T) {
	sealed, _, wantDocuments := persistenceFixture(t, true)
	root := t.TempDir()
	path := SegmentPaths{Dir: root + "/segment"}
	if err := SaveSegment(context.Background(), path, sealed, Options{}); err != nil {
		t.Fatal(err)
	}
	loaded, err := OpenSegment(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	view, err := semantic.NewReadView(1, []*semantic.Segment{loaded.Sealed.Segment})
	if err != nil {
		t.Fatal(err)
	}
	got, err := view.SearchDocuments(context.Background(), zeroQueryEncoder(), semantic.Document{ID: "query"}, len(wantDocuments.Hits))
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

func TestSealedSegmentAPIIsIndependentFromLegacyCallerContract(t *testing.T) {
	fixture, _, _ := persistenceFixture(t, true)
	sealed := SealedSegment{
		Segment:                 fixture.Segment,
		Space:                   fixture.Space,
		Chunking:                fixture.Chunking,
		MaxAllocatedVectorID:    fixture.MaxAllocatedVectorID,
		MaxK:                    fixture.MaxK,
		MaxChunkCandidates:      fixture.MaxChunkCandidates,
		MaxChunksPerDocumentHit: fixture.MaxChunksPerDocumentHit,
	}
	path := SegmentPaths{Dir: t.TempDir() + "/segment"}
	if err := SaveSealedSegment(context.Background(), path, sealed, Options{}); err != nil {
		t.Fatal(err)
	}
	loaded, err := OpenSealedSegment(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Sealed.Segment == nil || loaded.Sealed.Segment.ComponentID() != sealed.Segment.ComponentID() {
		t.Fatalf("loaded sealed segment = %+v", loaded.Sealed)
	}
	if loaded.Sealed.MaxAllocatedVectorID != sealed.MaxAllocatedVectorID {
		t.Fatalf("loaded max vector ID = %d, want %d", loaded.Sealed.MaxAllocatedVectorID, sealed.MaxAllocatedVectorID)
	}
	root := t.TempDir()
	generation, err := PublishSealedSegment(context.Background(), root, 1, sealed, Options{})
	if err != nil {
		t.Fatal(err)
	}
	opened, err := Open(root, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if generation.ID != 1 || opened.Sealed.Segment == nil {
		t.Fatalf("published sealed generation = %d, segment = %+v", generation.ID, opened.Sealed)
	}
}
