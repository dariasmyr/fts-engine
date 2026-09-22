package semantic

import (
	"context"
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

func TestSegmentAccessorsAndSearch(t *testing.T) {
	segment := testImmutableSegment(t)
	if segment.Kind() != SegmentKindChunkHNSW || segment.Vectors() == nil || segment.Searcher() == nil {
		t.Fatalf("segment accessors = kind %d, vectors %p, searcher %p", segment.Kind(), segment.Vectors(), segment.Searcher())
	}
	result, err := segment.Search(context.Background(), []float32{0, 0}, 2, vector.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 2 || segment.Rows()[result.Hits[0].Ordinal].Chunk.DocID != "doc-00" {
		t.Fatalf("segment result = %+v", result)
	}
	if err := segment.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestReadViewPublishesImmutableSegments(t *testing.T) {
	segment := testImmutableSegment(t)
	view, err := NewReadView(7, []*Segment{segment})
	if err != nil {
		t.Fatal(err)
	}
	if view.Generation() != 7 || view.SegmentCount() != 1 || view.LiveVectorCount() != segment.Len() {
		t.Fatalf("view metadata = generation %d, segments %d, live %d", view.Generation(), view.SegmentCount(), view.LiveVectorCount())
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentValidationRejectsDuplicateRows(t *testing.T) {
	segment := testImmutableSegment(t)
	rows := segment.Rows()
	rows[1].VectorID = rows[0].VectorID
	invalid := &Segment{component: segment.component, metadata: segment.metadata, rows: rows, searcher: segment.searcher}
	if err := invalid.Validate(); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("duplicate row error = %v", err)
	}
}

func testImmutableSegment(t *testing.T) *Segment {
	t.Helper()
	config := testConfig(10, 100)
	vectorSpace, err := config.Embedding.VectorSpace()
	if err != nil {
		t.Fatal(err)
	}
	values := [][]float32{{0, 0}, {1, 0}, {2, 0}}
	source, err := vector.NewMemorySource(vectorSpace, values)
	if err != nil {
		t.Fatal(err)
	}
	rows := []VectorRow{
		{VectorID: 1, Chunk: testChunk("doc-00", "chunk-00", 0, values[0]).Ref},
		{VectorID: 2, Chunk: testChunk("doc-01", "chunk-01", 0, values[1]).Ref},
		{VectorID: 3, Chunk: testChunk("doc-02", "chunk-02", 0, values[2]).Ref},
	}
	segment, err := BuildSegment(context.Background(), MutableHeadID, SegmentMetadata{Embedding: config.Embedding, Chunking: config.Chunking}, source, rows, hnsw.BuildOptions{
		BuildConfig: hnsw.BuildConfig{
			Dimensions: 2, Metric: vector.MetricL2Squared, MaxVectors: 3,
			MaxVectorBytes: 3 * 2 * 4, MaxNeighbors: 4, EfConstruction: 16, Seed: 11,
		},
		SearchConfig: hnsw.SearchConfig{
			DefaultEfSearch: 3, MaxEfSearch: 3, DefaultVisitLimit: 3, MaxVisitLimit: 3, MaxK: 3,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return segment
}
