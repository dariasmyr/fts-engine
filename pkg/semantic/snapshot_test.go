package semantic

import (
	"context"
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

func TestCommittedSnapshotRejectsPendingMutations(t *testing.T) {
	service := newTestService(t)
	batch := []ChunkVector{testChunk("doc-a", "a-1", 0, []float32{1, 0})}
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
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc-a", "a-1", 0, []float32{1, 0})}); err != nil {
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
	invalidComponent.NextComponentID = segment.Snapshot().ComponentID()
	if _, err := Hydrate(ctx, invalidComponent); !errors.Is(err, ErrInternalState) {
		t.Fatalf("invalid component watermark error = %v", err)
	}
	invalidLiveness := base
	invalidLiveness.Segments = []HydratedSegment{{Snapshot: segment.Snapshot(), LivenessWords: nil}}
	if _, err := Hydrate(ctx, invalidLiveness); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("invalid liveness error = %v", err)
	}
	unnormalized := base
	unnormalized.Config.HNSWSearch = hnsw.SearchConfig{}
	if _, err := Hydrate(ctx, unnormalized); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unnormalized config error = %v", err)
	}
}
