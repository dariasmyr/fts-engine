package semantic

import (
	"context"
	"errors"
	"testing"
)

func TestCommittedStateRejectsPendingMutations(t *testing.T) {
	service := newTestService(t)
	batch := []EncodedChunk{testChunk("doc-a", "a-1", 0, []float32{1, 0})}
	if err := service.addEncodedDocument(context.Background(), "doc-a", batch); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CommittedState(context.Background()); !errors.Is(err, ErrPendingMutations) {
		t.Fatalf("CommittedState error = %v, want %v", err, ErrPendingMutations)
	}
}

func TestCommittedSegmentReturnsDefensiveCopies(t *testing.T) {
	ctx := context.Background()
	service := newTestService(t)
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-a", "a-1", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	state, err := service.CommittedState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	segment := state.Segments()[0]
	data := segment.Data()
	data.Rows[0].VectorID = 99
	if got := segment.Data().Rows[0].VectorID; got == 99 {
		t.Fatal("Data returned caller-mutable rows")
	}
	livenessWords := segment.LivenessWords()
	livenessWords[0] = 0
	if got := segment.LivenessWords()[0]; got == 0 {
		t.Fatal("LivenessWords returned caller-mutable words")
	}
}

func TestRestoreRejectsInvalidWatermarksAndLiveness(t *testing.T) {
	ctx := context.Background()
	service := newTestService(t)
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-a", "a-1", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.CommittedState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	segment := snapshot.Segments()[0]
	base := RestoreState{
		Config:               snapshot.Config(),
		Revision:             snapshot.Revision(),
		MaxAllocatedVectorID: snapshot.MaxAllocatedVectorID(),
		NextComponentID:      snapshot.NextComponentID(),
		Segments:             []StoredSegment{{Data: segment.Data(), LivenessWords: segment.LivenessWords()}},
	}

	invalidID := base
	invalidID.MaxAllocatedVectorID = 0
	if _, err := Restore(ctx, invalidID); !errors.Is(err, ErrInternalState) {
		t.Fatalf("invalid vector watermark error = %v", err)
	}
	invalidComponent := base
	invalidComponent.NextComponentID = segment.Data().ComponentID
	if _, err := Restore(ctx, invalidComponent); !errors.Is(err, ErrInternalState) {
		t.Fatalf("invalid component watermark error = %v", err)
	}
	invalidLiveness := base
	invalidLiveness.Segments = []StoredSegment{{Data: segment.Data(), LivenessWords: nil}}
	if _, err := Restore(ctx, invalidLiveness); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("invalid liveness error = %v", err)
	}
	unnormalized := base
	unnormalized.Config.HNSW.DefaultEfSearch = 0
	if _, err := Restore(ctx, unnormalized); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unnormalized config error = %v", err)
	}
}

func TestRestoreRejectsSegmentsOutsideVectorIDOrder(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-a", "a-1", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-b", "b-1", 0, []float32{2, 0})}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.CommittedState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	segments := snapshot.Segments()
	state := RestoreState{
		Config:               snapshot.Config(),
		Revision:             snapshot.Revision(),
		MaxAllocatedVectorID: snapshot.MaxAllocatedVectorID(),
		NextComponentID:      snapshot.NextComponentID(),
		Segments: []StoredSegment{
			{Data: segments[1].Data(), LivenessWords: segments[1].LivenessWords()},
			{Data: segments[0].Data(), LivenessWords: segments[0].LivenessWords()},
		},
	}

	if _, err := Restore(ctx, state); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("Restore error = %v, want %v", err, ErrInvalidSegment)
	}
}

func TestRestoreRejectsNonContiguousDocumentVersion(t *testing.T) {
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
	snapshot, err := service.CommittedState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	committed := snapshot.Segments()[0]
	data := committed.Data()
	data.Rows[1].Chunk.DocID = "doc-b"
	data.Rows[2].Chunk.DocID = "doc-a"
	state := RestoreState{
		Config:               snapshot.Config(),
		Revision:             snapshot.Revision(),
		MaxAllocatedVectorID: snapshot.MaxAllocatedVectorID(),
		NextComponentID:      snapshot.NextComponentID(),
		Segments: []StoredSegment{{
			Data:          data,
			LivenessWords: committed.LivenessWords(),
		}},
	}

	if _, err := Restore(ctx, state); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("Restore error = %v, want %v", err, ErrInvalidSegment)
	}
}

func TestRestoreRetainsLocationsOnlyForLiveRows(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc", "old", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := service.replaceEncodedDocument(ctx, "doc", []EncodedChunk{testChunk("doc", "new", 0, []float32{2, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.CommittedState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	segments := snapshot.Segments()
	storedSegments := make([]StoredSegment, len(segments))
	for i, segment := range segments {
		storedSegments[i] = StoredSegment{Data: segment.Data(), LivenessWords: segment.LivenessWords()}
	}
	restored, err := Restore(ctx, RestoreState{
		Config:               snapshot.Config(),
		Revision:             snapshot.Revision(),
		MaxAllocatedVectorID: snapshot.MaxAllocatedVectorID(),
		NextComponentID:      snapshot.NextComponentID(),
		Segments:             storedSegments,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := restored.locations[1]; exists {
		t.Fatal("restore retained a stale vector location")
	}
	if _, exists := restored.locations[2]; !exists || len(restored.locations) != 1 {
		t.Fatalf("restored live locations = %v", restored.locations)
	}
}

func TestRestoreRejectsDuplicateChunkIdentityIncludingDeadRows(t *testing.T) {
	ctx := context.Background()
	service := newTestService(t)
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-a", "chunk", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-b", "other", 0, []float32{0, 1})}); err != nil {
		t.Fatal(err)
	}
	state, err := service.CommittedState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	segments := state.Segments()
	first := segments[0].Data()
	second := segments[1].Data()
	second.Rows[0].Chunk.DocID = first.Rows[0].Chunk.DocID
	second.Rows[0].Chunk.ID = first.Rows[0].Chunk.ID

	_, err = Restore(ctx, RestoreState{
		Config:               state.Config(),
		Revision:             state.Revision(),
		MaxAllocatedVectorID: state.MaxAllocatedVectorID(),
		NextComponentID:      state.NextComponentID(),
		Segments: []StoredSegment{
			{Data: first, LivenessWords: []uint64{0}},
			{Data: second, LivenessWords: []uint64{0}},
		},
	})
	if !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("Restore duplicate dead chunk error = %v, want %v", err, ErrInvalidSegment)
	}
}

func TestRestoreRejectsMismatchedIndexAndVectorSource(t *testing.T) {
	ctx := context.Background()
	service := newTestService(t)
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-a", "a", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-b", "b", 0, []float32{0, 1})}); err != nil {
		t.Fatal(err)
	}
	state, err := service.CommittedState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	segments := state.Segments()
	first := segments[0].Data()
	first.Vectors = segments[1].Data().Vectors

	_, err = Restore(ctx, RestoreState{
		Config:               state.Config(),
		Revision:             state.Revision(),
		MaxAllocatedVectorID: state.MaxAllocatedVectorID(),
		NextComponentID:      state.NextComponentID(),
		Segments:             []StoredSegment{{Data: first, LivenessWords: segments[0].LivenessWords()}},
	})
	if !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("Restore mismatched source error = %v, want %v", err, ErrInvalidSegment)
	}
}

func TestRestoreDefensivelyCopiesRows(t *testing.T) {
	ctx := context.Background()
	service := newTestService(t)
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-a", "a-1", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.CommittedState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	committed := snapshot.Segments()[0]
	data := committed.Data()
	rows := data.Rows
	livenessWords := committed.LivenessWords()
	restored, err := Restore(ctx, RestoreState{
		Config: snapshot.Config(), Revision: snapshot.Revision(), MaxAllocatedVectorID: snapshot.MaxAllocatedVectorID(),
		NextComponentID: snapshot.NextComponentID(),
		Segments:        []StoredSegment{{Data: data, LivenessWords: livenessWords}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if &restored.published.segments[0].segment.rows[0] == &rows[0] {
		t.Fatal("Restore retained caller-owned rows")
	}
	livenessWords[0] = 0
	if !restored.published.segments[0].filter.Allows(0) {
		t.Fatal("Restore retained caller-owned liveness words")
	}
}
