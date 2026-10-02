package semantic

import (
	"context"
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

func TestSegmentAccessorsAndSearch(t *testing.T) {
	segment := testImmutableSegment(t)
	if segment.vectorStore() == nil || segment.hnswIndex() == nil {
		t.Fatalf("segment accessors = vectors %p, index %p", segment.vectorStore(), segment.hnswIndex())
	}
	result, err := segment.searchVectors(context.Background(), []float32{0, 0}, 2, vector.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 2 || segment.rowsCopy()[result.Hits[0].Ordinal].Chunk.DocID != "doc-00" {
		t.Fatalf("segment result = %+v", result)
	}
	if err := segment.validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentValidationRejectsDuplicateRows(t *testing.T) {
	s := testImmutableSegment(t)
	rows := s.rowsCopy()
	rows[1].VectorID = rows[0].VectorID
	invalid := &segment{
		component:  s.component,
		descriptor: s.descriptor,
		rows:       rows,
		vectors:    s.vectors,
		index:      s.index,
		search:     s.search,
	}
	if err := invalid.validate(); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("duplicate row error = %v", err)
	}
}

func TestHydrateRejectsDuplicateRows(t *testing.T) {
	segment := testImmutableSegment(t)
	rows := segment.rowsCopy()
	rows[1].VectorID = rows[0].VectorID
	snapshot := NewSegmentSnapshot(segment.componentID(), segment.pipelineDescriptor(), rows, segment.vectorStore(), segment.hnswIndex())
	if _, err := hydrateTestSegment(snapshot); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("Hydrate error = %v", err)
	}
}

func TestHydrateRejectsDifferentVectorSource(t *testing.T) {
	segment := testImmutableSegment(t)
	calculator, err := segment.pipelineDescriptor().Embedding.Calculator()
	if err != nil {
		t.Fatal(err)
	}
	vectors, err := vectorstore.NewMemoryVectorStore(calculator, [][]float32{{9, 9}, {8, 8}, {7, 7}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := NewSegmentSnapshot(segment.componentID(), segment.pipelineDescriptor(), segment.rowsCopy(), vectors, segment.index)
	if _, err := hydrateTestSegment(snapshot); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("different source error = %v, want ErrInvalidSegment", err)
	}
}

func TestSegmentSnapshotDefensivelyCopiesRows(t *testing.T) {
	segment := testImmutableSegment(t)
	snapshot := segment.snapshot()
	rows := snapshot.Rows()
	rows[0].VectorID = 99
	if snapshot.Rows()[0].VectorID == 99 {
		t.Fatal("SegmentSnapshot.Rows exposed mutable storage")
	}
}

func testImmutableSegment(t *testing.T) *segment {
	t.Helper()
	config := testConfig(10, 100)
	calculator, err := config.Embedding.Calculator()
	if err != nil {
		t.Fatal(err)
	}
	values := [][]float32{{0, 0}, {1, 0}, {2, 0}}
	source, err := vectorstore.NewMemoryVectorStore(calculator, values)
	if err != nil {
		t.Fatal(err)
	}
	rows := []VectorRow{
		{VectorID: 1, Chunk: testChunk("doc-00", "chunk-00", 0, values[0]).Ref},
		{VectorID: 2, Chunk: testChunk("doc-01", "chunk-01", 0, values[1]).Ref},
		{VectorID: 3, Chunk: testChunk("doc-02", "chunk-02", 0, values[2]).Ref},
	}
	segment, err := buildSegment(context.Background(), 1, PipelineDescriptor{Embedding: config.Embedding, Chunking: config.Chunking}, source, rows, hnsw.BuildOptions{
		Build: hnsw.BuildConfig{
			Dimensions: 2, Metric: vector.MetricL2Squared, MaxVectors: 3,
			MaxVectorBytes: 3 * 2 * 4, MaxNeighbors: 4, EfConstruction: 16, Seed: 11,
		},
		Search: hnsw.SearchConfig{
			DefaultEfSearch: 3, MaxEfSearch: 3, DefaultVisitLimit: 3, MaxVisitLimit: 3, MaxK: 3,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return segment
}

func hydrateTestSegment(snapshot SegmentSnapshot) (*Service, error) {
	config, err := testConfig(3, 3).normalized()
	if err != nil {
		return nil, err
	}
	return Hydrate(context.Background(), HydrationState{
		Config:               config,
		Revision:             1,
		MaxAllocatedVectorID: 3,
		NextComponentID:      2,
		Segments:             []HydratedSegment{{Snapshot: snapshot, LivenessWords: []uint64{7}}},
	})
}
