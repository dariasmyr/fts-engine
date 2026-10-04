package semantic

import (
	"context"
	"errors"
	"testing"
)

func TestCommittedSnapshotRejectsPendingMutations(t *testing.T) {
	service := newTestService(t)
	batch := []EncodedChunk{testChunk("doc-a", "a-1", 0, []float32{1, 0})}
	if err := service.addEncodedDocument(context.Background(), "doc-a", batch); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CommittedSnapshot(context.Background()); !errors.Is(err, ErrPendingMutations) {
		t.Fatalf("CommittedSnapshot error = %v, want %v", err, ErrPendingMutations)
	}
}

func TestHydrateRejectsInvalidWatermarksAndLiveness(t *testing.T) {
	ctx := context.Background()
	service := newTestService(t)
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-a", "a-1", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.CommittedSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	segment := snapshot.Segments()[0]
	base := HydrationState{
		Config:               snapshot.Config(),
		Revision:             snapshot.Revision(),
		MaxAllocatedVectorID: snapshot.MaxAllocatedVectorID(),
		NextComponentID:      snapshot.NextComponentID(),
		Segments:             []HydratedSegment{{Snapshot: segment.Snapshot(), LivenessWords: segment.LivenessWords()}},
	}

	invalidID := base
	invalidID.MaxAllocatedVectorID = 0
	if _, err := Hydrate(ctx, invalidID); !errors.Is(err, ErrInternalState) {
		t.Fatalf("invalid vector watermark error = %v", err)
	}
	invalidComponent := base
	invalidComponent.NextComponentID = segment.Snapshot().Data().ComponentID
	if _, err := Hydrate(ctx, invalidComponent); !errors.Is(err, ErrInternalState) {
		t.Fatalf("invalid component watermark error = %v", err)
	}
	invalidLiveness := base
	invalidLiveness.Segments = []HydratedSegment{{Snapshot: segment.Snapshot(), LivenessWords: nil}}
	if _, err := Hydrate(ctx, invalidLiveness); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("invalid liveness error = %v", err)
	}
	unnormalized := base
	unnormalized.Config.HNSW.DefaultEfSearch = 0
	if _, err := Hydrate(ctx, unnormalized); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unnormalized config error = %v", err)
	}
}

func TestHydrateRejectsSegmentsOutsideVectorIDOrder(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-a", "a-1", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-b", "b-1", 0, []float32{2, 0})}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.CommittedSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	segments := snapshot.Segments()
	state := HydrationState{
		Config:               snapshot.Config(),
		Revision:             snapshot.Revision(),
		MaxAllocatedVectorID: snapshot.MaxAllocatedVectorID(),
		NextComponentID:      snapshot.NextComponentID(),
		Segments: []HydratedSegment{
			{Snapshot: segments[1].Snapshot(), LivenessWords: segments[1].LivenessWords()},
			{Snapshot: segments[0].Snapshot(), LivenessWords: segments[0].LivenessWords()},
		},
	}

	if _, err := Hydrate(ctx, state); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("Hydrate error = %v, want %v", err, ErrInvalidSegment)
	}
}

func TestHydrateRejectsNonContiguousDocumentVersion(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := service.addEncodedDocument(ctx, "doc-a", []EncodedChunk{
		testChunk("doc-a", "a-1", 0, []float32{1, 0}),
		testChunk("doc-a", "a-2", 1, []float32{2, 0}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.addEncodedDocument(ctx, "doc-b", []EncodedChunk{testChunk("doc-b", "b-1", 0, []float32{3, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.CommittedSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	committed := snapshot.Segments()[0]
	data := committed.Snapshot().Data()
	data.Rows[1].Chunk.DocID = "doc-b"
	data.Rows[2].Chunk.DocID = "doc-a"
	state := HydrationState{
		Config:               snapshot.Config(),
		Revision:             snapshot.Revision(),
		MaxAllocatedVectorID: snapshot.MaxAllocatedVectorID(),
		NextComponentID:      snapshot.NextComponentID(),
		Segments: []HydratedSegment{{
			Snapshot:      NewSegmentSnapshot(data),
			LivenessWords: committed.LivenessWords(),
		}},
	}

	if _, err := Hydrate(ctx, state); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("Hydrate error = %v, want %v", err, ErrInvalidSegment)
	}
}
