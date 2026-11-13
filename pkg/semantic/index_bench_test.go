package semantic

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

const (
	benchmarkDimensions = 16
	benchmarkDocuments  = 4096
)

func BenchmarkIndexSearchSmallCandidateBudget(b *testing.B) {
	index := newBenchmarkIndex(b, benchmarkDocuments, benchmarkDocuments, 16)
	addBenchmarkDocuments(b, index, 0, benchmarkDocuments)
	if err := index.Commit(context.Background()); err != nil {
		b.Fatal(err)
	}
	query := []ChunkEmbedding{benchmarkChunk("query", 0)}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := index.Search(context.Background(), query, 10, QueryConfig{
			CandidateChunks: 16,
		})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Hits) == 0 {
			b.Fatal("Search() returned no hits")
		}
	}
}

func BenchmarkIndexSearchMultiSegmentSmallCandidateBudget(b *testing.B) {
	const segmentCount = 8
	index := newBenchmarkIndex(b, benchmarkDocuments, benchmarkDocuments, segmentCount+1)
	documentsPerSegment := benchmarkDocuments / segmentCount
	for segment := range segmentCount {
		addBenchmarkDocuments(
			b,
			index,
			segment*documentsPerSegment,
			documentsPerSegment,
		)
		if err := index.Commit(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
	query := []ChunkEmbedding{benchmarkChunk("query", 0)}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := index.Search(context.Background(), query, 10, QueryConfig{
			CandidateChunks: 16,
		})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Hits) == 0 {
			b.Fatal("Search() returned no hits")
		}
	}
}

func BenchmarkIndexCommit(b *testing.B) {
	const documents = 256

	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		index := newBenchmarkIndex(b, documents, documents, 16)
		addBenchmarkDocuments(b, index, 0, documents)
		b.StartTimer()
		if err := index.Commit(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIndexCompact(b *testing.B) {
	const documents = 256

	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		index := newBenchmarkIndex(b, documents, documents, 16)
		addBenchmarkDocuments(b, index, 0, documents)
		if err := index.Commit(context.Background()); err != nil {
			b.Fatal(err)
		}
		for n := range documents {
			docID := fts.DocID(fmt.Sprintf("doc-%06d", n))
			if err := index.ReplaceDocument(context.Background(), docID, []ChunkEmbedding{
				benchmarkChunk(docID, n+documents),
			}); err != nil {
				b.Fatal(err)
			}
		}
		if err := index.Commit(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := index.Compact(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func newBenchmarkIndex(tb testing.TB, maxLive, maxStale, maxSegments int) *Index {
	tb.Helper()
	index, err := NewIndex(benchmarkConfig(maxLive, maxStale, maxSegments))
	if err != nil {
		tb.Fatal(err)
	}
	return index
}

func benchmarkConfig(maxLive, maxStale, maxSegments int) IndexConfig {
	return IndexConfig{
		Dimensions: benchmarkDimensions,
		Metric:     vector.MetricL2Squared,
		Build:      hnsw.BuildConfig{MaxNeighbors: 16, EfConstruction: 64, Seed: 1},
		Operational: OperationalLimits{
			Capacity:       CapacityLimits{MaxLiveVectors: maxLive, MaxStaleVectors: maxStale, MaxSegments: maxSegments},
			Ingestion:      IngestionLimits{MaxChunksPerDocument: 1},
			Search:         SearchLimits{MaxResults: 10, MaxCandidateChunks: 64, MaxChunksPerResult: 1},
			SearchDefaults: hnsw.SearchConfig{EfSearch: 32, VisitLimit: maxLive},
		},
	}
}

func addBenchmarkDocuments(tb testing.TB, index *Index, start, count int) {
	tb.Helper()
	for offset := range count {
		n := start + offset
		docID := fts.DocID(fmt.Sprintf("doc-%06d", n))
		if err := index.AddDocument(context.Background(), docID, []ChunkEmbedding{
			benchmarkChunk(docID, n),
		}); err != nil {
			tb.Fatal(err)
		}
	}
}

func benchmarkChunk(docID fts.DocID, n int) ChunkEmbedding {
	values := make([]float32, benchmarkDimensions)
	for dimension := range values {
		values[dimension] = float32(math.Sin(float64((n + 1) * (dimension + 1))))
	}
	return ChunkEmbedding{
		Ref:       Ref{ID: ChunkID(fmt.Sprintf("chunk-%06d", n)), DocID: docID, Field: fts.DefaultField, EndByte: 1},
		Embedding: values,
	}
}
