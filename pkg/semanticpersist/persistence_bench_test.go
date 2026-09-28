package semanticpersist

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

func BenchmarkSaveSealedSegment(b *testing.B) {
	for _, test := range semanticPersistenceBenchmarkCases() {
		b.Run(test.name, func(b *testing.B) {
			fixture := newSemanticPersistenceBenchmarkFixture(b, test.rows, test.dimensions)
			root := b.TempDir()

			b.ReportAllocs()
			b.SetBytes(fixture.persistedBytes)
			b.ResetTimer()

			for iteration := 0; b.Loop(); iteration++ {
				path := filepath.Join(root, fmt.Sprintf("segment-%d", iteration))
				if err := SaveSealedSegment(context.Background(), SegmentPaths{Dir: path}, fixture.sealed, Options{Durability: DurabilityAsynchronous}); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if err := os.RemoveAll(path); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			b.ReportMetric(float64(fixture.persistedBytes), "persisted-bytes/op")
		})
	}
}

func BenchmarkOpenSealedSegment(b *testing.B) {
	for _, test := range semanticPersistenceBenchmarkCases() {
		b.Run(test.name, func(b *testing.B) {
			fixture := newSemanticPersistenceBenchmarkFixture(b, test.rows, test.dimensions)
			path := filepath.Join(b.TempDir(), "segment")
			if err := SaveSealedSegment(context.Background(), SegmentPaths{Dir: path}, fixture.sealed, Options{Durability: DurabilityAsynchronous}); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.SetBytes(fixture.persistedBytes)
			b.ResetTimer()

			for b.Loop() {
				loaded, err := OpenSealedSegment(SegmentPaths{Dir: path}, OpenOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if err := loaded.Sealed.Segment.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(fixture.persistedBytes), "persisted-bytes/op")
		})
	}
}

func BenchmarkPublishSemanticGeneration(b *testing.B) {
	for _, durability := range []DurabilityMode{DurabilityAsynchronous, DurabilitySynchronous} {
		for _, test := range semanticPersistenceBenchmarkCases() {
			b.Run(fmt.Sprintf("%s/%s", durabilityName(durability), test.name), func(b *testing.B) {
				fixture := newSemanticPersistenceBenchmarkFixture(b, test.rows, test.dimensions)
				root := b.TempDir()

				b.ReportAllocs()
				b.SetBytes(fixture.persistedBytes)
				b.ResetTimer()

				for generation := uint64(1); b.Loop(); generation++ {
					path := filepath.Join(root, fmt.Sprintf("store-%d", generation))
					if _, err := Publish(context.Background(), path, generation, fixture.sealed, Options{Durability: durability}); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					if err := os.RemoveAll(path); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
				b.ReportMetric(float64(fixture.persistedBytes), "persisted-bytes/op")
			})
		}
	}
}

func BenchmarkOpenSemanticGeneration(b *testing.B) {
	for _, test := range semanticPersistenceBenchmarkCases() {
		b.Run(test.name, func(b *testing.B) {
			fixture := newSemanticPersistenceBenchmarkFixture(b, test.rows, test.dimensions)
			root := filepath.Join(b.TempDir(), "store")
			if _, err := Publish(context.Background(), root, 1, fixture.sealed, Options{Durability: DurabilityAsynchronous}); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.SetBytes(fixture.persistedBytes)
			b.ResetTimer()

			for b.Loop() {
				loaded, err := Open(root, OpenOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if err := loaded.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(fixture.persistedBytes), "persisted-bytes/op")
		})
	}
}

type semanticPersistenceBenchmarkCase struct {
	name       string
	rows       int
	dimensions int
}

func semanticPersistenceBenchmarkCases() []semanticPersistenceBenchmarkCase {
	return []semanticPersistenceBenchmarkCase{
		{name: "rows=1000/dims=32", rows: 1_000, dimensions: 32},
		{name: "rows=1000/dims=384", rows: 1_000, dimensions: 384},
		{name: "rows=10000/dims=32", rows: 10_000, dimensions: 32},
		{name: "rows=10000/dims=384", rows: 10_000, dimensions: 384},
	}
}

type semanticPersistenceBenchmarkFixture struct {
	sealed         SealedSegment
	persistedBytes int64
}

func newSemanticPersistenceBenchmarkFixture(tb testing.TB, rows, dimensions int) semanticPersistenceBenchmarkFixture {
	tb.Helper()

	calculator, err := vector.NewCalculator(dimensions, vector.MetricL2Squared)
	if err != nil {
		tb.Fatal(err)
	}
	values := make([][]float32, rows)
	for row := range values {
		value := make([]float32, dimensions)
		for dimension := range value {
			value[dimension] = float32((row+1)*(dimension+3)%101) / 101
		}
		values[row] = value
	}
	source, err := vectorstore.NewMemoryVectorStore(calculator, values)
	if err != nil {
		tb.Fatal(err)
	}

	maxK := min(10, rows)
	index, err := hnsw.BuildIndex(context.Background(), source, hnsw.BuildOptions{
		BuildConfig: hnsw.BuildConfig{
			Dimensions: dimensions, Metric: vector.MetricL2Squared,
			MaxVectors: rows, MaxVectorBytes: uint64(rows * dimensions * 4),
			MaxNeighbors: 4, EfConstruction: 32, Seed: 17,
		},
		SearchConfig: hnsw.SearchConfig{
			DefaultEfSearch: min(64, rows), MaxEfSearch: rows,
			DefaultVisitLimit: rows, MaxVisitLimit: rows, MaxK: maxK,
		},
	})
	if err != nil {
		tb.Fatal(err)
	}

	embedding, err := semantic.NewEmbeddingDescriptor(
		"benchmark-provider", "benchmark-model", "v1", "benchmark-pipeline",
		dimensions, vector.MetricL2Squared, 1,
	)
	if err != nil {
		tb.Fatal(err)
	}
	rowsMetadata := make([]semantic.VectorRow, rows)
	for row := range rowsMetadata {
		rowsMetadata[row] = semantic.VectorRow{
			VectorID: semantic.VectorID(row + 1),
			Chunk: chunk.Ref{
				ID:      chunk.ID(fmt.Sprintf("chunk-%d", row)),
				DocID:   fts.DocID(fmt.Sprintf("doc-%d", row)),
				Field:   fts.DefaultField,
				EndByte: 1,
			},
		}
	}
	segment, err := semantic.NewSegment(2, semantic.SegmentMetadata{
		Embedding: embedding,
		Chunking:  semantic.ChunkingDescriptor{ID: "benchmark-chunks", Version: 1, Fingerprint: "benchmark-chunks-v1"},
	}, index, rowsMetadata)
	if err != nil {
		tb.Fatal(err)
	}

	fixture := semanticPersistenceBenchmarkFixture{sealed: SealedSegment{
		Segment:                 segment,
		MaxAllocatedVectorID:    semantic.VectorID(rows),
		MaxK:                    maxK,
		MaxChunkCandidates:      min(100, rows),
		MaxChunksPerDocumentHit: 3,
	}}
	fixture.persistedBytes = semanticPersistenceBenchmarkSize(tb, fixture.sealed)
	return fixture
}

func semanticPersistenceBenchmarkSize(tb testing.TB, sealed SealedSegment) int64 {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "segment")
	if err := SaveSealedSegment(context.Background(), SegmentPaths{Dir: path}, sealed, Options{Durability: DurabilityAsynchronous}); err != nil {
		tb.Fatal(err)
	}
	var size int64
	if err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	}); err != nil {
		tb.Fatal(err)
	}
	return size
}
