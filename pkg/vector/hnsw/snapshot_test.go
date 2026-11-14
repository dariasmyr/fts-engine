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
	config := hnsw.Config{
		Build:  hnsw.BuildConfig{MaxNeighbors: 2, EfConstruction: 4, Seed: 7},
		Search: hnsw.SearchConfig{EfSearch: 4, VisitLimit: 4},
	}
	index, err := hnsw.Build(context.Background(), vectors, config)
	if err != nil {
		t.Fatal(err)
	}

	want, err := index.Search(context.Background(), []float32{1.1, 0}, 4, hnsw.QueryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := hnsw.Snapshot(index)
	if len(snapshot.Levels) != vectors.Len() {
		t.Fatalf("snapshot nodes = %d, vector rows = %d", len(snapshot.Levels), vectors.Len())
	}
	restored, err := hnsw.Restore(snapshot, vectors, config)
	if err != nil {
		t.Fatal(err)
	}
	if restoredSnapshot := hnsw.Snapshot(restored); !reflect.DeepEqual(restoredSnapshot, snapshot) {
		t.Fatal("Snapshot/Restore changed the validated topology")
	}
	got, err := restored.Search(context.Background(), []float32{1.1, 0}, 4, hnsw.QueryConfig{})
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

func TestRestoreAcceptsOlderBuildAndRejectsUnknownVersions(t *testing.T) {
	t.Parallel()

	space, err := vector.NewSpace(1, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	vectors, err := memorystore.New(space, [][]float32{{0}, {1}})
	if err != nil {
		t.Fatal(err)
	}
	config := hnsw.Config{
		Build:  hnsw.BuildConfig{MaxNeighbors: 2, EfConstruction: 4, Seed: 7},
		Search: hnsw.SearchConfig{EfSearch: 4, VisitLimit: 4},
	}
	index, err := hnsw.Build(t.Context(), vectors, config)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := hnsw.Snapshot(index)
	currentVersion := snapshot.BuildVersion
	if currentVersion < 2 {
		t.Fatalf("current build version = %d, want at least 2", currentVersion)
	}

	snapshot.BuildVersion = currentVersion - 1
	if _, err := hnsw.Restore(snapshot, vectors, config); err != nil {
		t.Fatalf("Restore() older compatible build: %v", err)
	}
	snapshot.BuildVersion = currentVersion + 1
	if _, err := hnsw.Restore(snapshot, vectors, config); err == nil {
		t.Fatal("Restore() accepted an unknown future build version")
	}
	snapshot.BuildVersion = currentVersion
	snapshot.LevelGeneratorVersion++
	if _, err := hnsw.Restore(snapshot, vectors, config); err == nil {
		t.Fatal("Restore() accepted an unknown level generator version")
	}
}

func TestIndex_TopologyStats(t *testing.T) {
	t.Parallel()

	space, err := vector.NewSpace(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	vectors, err := memorystore.New(space, [][]float32{{0, 0}, {1, 0}, {3, 0}, {10, 0}})
	if err != nil {
		t.Fatal(err)
	}
	config := hnsw.Config{
		Build:  hnsw.BuildConfig{MaxNeighbors: 2, EfConstruction: 4, Seed: 7},
		Search: hnsw.SearchConfig{EfSearch: 4, VisitLimit: 4},
	}
	index, err := hnsw.Build(context.Background(), vectors, config)
	if err != nil {
		t.Fatal(err)
	}

	snapshot := hnsw.Snapshot(index)
	want := hnsw.TopologyStats{
		NodeCount:             len(snapshot.Levels),
		Level0LinkCount:       len(snapshot.Level0Links),
		UpperLayerNodeCount:   len(snapshot.UpperLinkOffsets) - 1,
		UpperLinkCount:        len(snapshot.UpperLinks),
		MaxNeighbors:          snapshot.MaxNeighbors,
		LevelZeroMaxNeighbors: snapshot.LevelZeroMaxNeighbors,
		EfConstruction:        snapshot.EfConstruction,
		Seed:                  snapshot.Seed,
	}
	if got := index.TopologyStats(); got != want {
		t.Fatalf("TopologyStats() = %+v, want %+v", got, want)
	}
	if got := (*hnsw.Index)(nil).TopologyStats(); got != (hnsw.TopologyStats{}) {
		t.Fatalf("nil TopologyStats() = %+v", got)
	}
}
