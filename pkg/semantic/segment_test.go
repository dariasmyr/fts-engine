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
	if segment.Kind() != SegmentKindChunkHNSW || segment.Vectors() == nil || segment.Index() == nil {
		t.Fatalf("segment accessors = kind %d, vectors %p, index %p", segment.Kind(), segment.Vectors(), segment.Index())
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

func TestSegmentValidationRejectsDuplicateRows(t *testing.T) {
	segment := testImmutableSegment(t)
	rows := segment.Rows()
	rows[1].VectorID = rows[0].VectorID
	invalid := &Segment{
		component: segment.component,
		metadata:  segment.metadata,
		rows:      rows,
		vectors:   segment.vectors,
		index:     segment.index,
		search:    segment.search,
	}
	if err := invalid.Validate(); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("duplicate row error = %v", err)
	}
}

func TestNewSegmentRejectsDuplicateRows(t *testing.T) {
	segment := testImmutableSegment(t)
	rows := segment.Rows()
	rows[1].VectorID = rows[0].VectorID
	if _, err := NewSegment(context.Background(), segment.ComponentID(), segment.Metadata(), segment.Vectors(), segment.Index(), rows); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("NewSegment error = %v", err)
	}
}

func TestNewSegmentRejectsDifferentVectorSource(t *testing.T) {
	segment := testImmutableSegment(t)
	calculator, err := segment.Metadata().Embedding.Calculator()
	if err != nil {
		t.Fatal(err)
	}
	vectors, err := vectorstore.NewMemoryVectorStore(calculator, [][]float32{{9, 9}, {8, 8}, {7, 7}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewSegment(context.Background(), segment.ComponentID(), segment.Metadata(), vectors, segment.index, segment.Rows()); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("different source error = %v, want ErrInvalidSegment", err)
	}
}

func testImmutableSegment(t *testing.T) *Segment {
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
	segment, err := BuildSegment(context.Background(), MutableHeadID, SegmentMetadata{Embedding: config.Embedding, Chunking: config.Chunking}, source, rows, hnsw.BuildOptions{
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
