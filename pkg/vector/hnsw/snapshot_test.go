package hnsw_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/internal/memorystore"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

func TestSnapshotRestorePreservesSearchAndOrdinals(t *testing.T) {
	t.Parallel()

	space, err := vector.NewSpace(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	vectors, err := memorystore.New(space, [][]float32{
		{0, 0},
		{1, 0},
		{3, 0},
		{10, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	search := hnsw.SearchConfig{EfSearch: 4, VisitLimit: 4}
	index, err := hnsw.Build(context.Background(), vectors, hnsw.BuildConfig{
		MaxNeighbors:   2,
		EfConstruction: 4,
		Seed:           7,
	}, search)
	if err != nil {
		t.Fatal(err)
	}

	want, err := index.Search(context.Background(), []float32{1.1, 0}, 4, hnsw.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := hnsw.Snapshot(index)
	if len(snapshot.Levels) != vectors.Len() {
		t.Fatalf("snapshot nodes = %d, vector rows = %d", len(snapshot.Levels), vectors.Len())
	}
	restored, err := hnsw.Restore(snapshot, vectors, search)
	if err != nil {
		t.Fatal(err)
	}
	got, err := restored.Search(context.Background(), []float32{1.1, 0}, 4, hnsw.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("restored search = %+v, want %+v", got, want)
	}

	snapshot.Levels[0] = 63
	if hnsw.Snapshot(index).Levels[0] == 63 {
		t.Fatal("snapshot aliases immutable index topology")
	}
}
