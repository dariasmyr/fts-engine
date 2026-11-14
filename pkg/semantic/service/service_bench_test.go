package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func BenchmarkServiceCommit(b *testing.B) {
	const documents = 256
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		encoder := &benchmarkEncoder{}
		index, err := semantic.NewIndex(semantic.IndexConfig{
			Dimensions: 2,
			Metric:     vector.MetricL2Squared,
			Operational: semantic.OperationalLimits{
				Capacity:  semantic.CapacityLimits{MaxLiveVectors: documents},
				Ingestion: semantic.IngestionLimits{MaxChunksPerDocument: 1},
				Search:    semantic.SearchLimits{MaxResults: 10, MaxCandidateChunks: 64, MaxChunksPerResult: 1},
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		service, err := NewService(index, encoder, encoder)
		if err != nil {
			b.Fatal(err)
		}
		for n := range documents {
			docID := fts.DocID(fmt.Sprintf("doc-%06d", n))
			if err := service.AddDocument(context.Background(), fts.Document{
				ID: docID, Fields: map[string]fts.FieldData{fts.DefaultField: {Text: "document"}},
			}); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		if err := service.Commit(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

type benchmarkEncoder struct{ next int }

func (e *benchmarkEncoder) EncodeDocument(_ context.Context, document fts.Document) ([]semantic.ChunkEmbedding, error) {
	chunk := benchmarkChunk(document.ID, e.next)
	e.next++
	return []semantic.ChunkEmbedding{chunk}, nil
}

func (e *benchmarkEncoder) EncodeQuery(context.Context, string) ([]semantic.ChunkEmbedding, error) {
	return []semantic.ChunkEmbedding{benchmarkChunk("query", 0)}, nil
}

func (e *benchmarkEncoder) EncodeQueryDocument(context.Context, fts.Document) ([]semantic.ChunkEmbedding, error) {
	return []semantic.ChunkEmbedding{benchmarkChunk("query", 0)}, nil
}

func benchmarkChunk(docID fts.DocID, n int) semantic.ChunkEmbedding {
	return semantic.ChunkEmbedding{
		Ref:       semantic.Ref{ID: semantic.ChunkID(fmt.Sprintf("chunk-%06d", n)), DocID: docID, Field: fts.DefaultField, EndByte: 1},
		Embedding: []float32{float32(n), 1},
	}
}
