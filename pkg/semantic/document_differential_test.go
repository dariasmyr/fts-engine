package semantic_test

import (
	"context"
	"math"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type differentialEncoder struct {
	descriptor semantic.Schema
	vectors    map[fts.DocID][]semantic.EncodedChunk
}

func (e differentialEncoder) Descriptor() semantic.Schema { return e.descriptor }

func (e differentialEncoder) Encode(_ context.Context, document fts.Document) ([]semantic.EncodedChunk, error) {
	result := slices.Clone(e.vectors[document.ID])
	for i := range result {
		result[i].Vector = slices.Clone(result[i].Vector)
	}
	return result, nil
}

func TestDocumentSearchDifferentialThroughLifecycle(t *testing.T) {
	ctx := context.Background()
	embedding, err := semantic.NewEmbeddingDescriptor("test", "model", "v1", "pipeline-v1", 2, vector.MetricL2Squared, 1)
	if err != nil {
		t.Fatal(err)
	}
	config := semantic.Config{
		Schema: semantic.Schema{
			Embedding: embedding,
			Chunking:  semantic.ChunkingDescriptor{ID: "chunks", Version: 1, Fingerprint: "chunks-v1"},
		},
		Limits: semantic.Limits{
			MaxLiveVectors:          64,
			MaxChunksPerDocument:    4,
			MaxDocumentsPerSearch:   4,
			MaxChunkCandidates:      64,
			MaxChunksPerDocumentHit: 2,
		},
	}
	service, err := semantic.New(config)
	if err != nil {
		t.Fatal(err)
	}
	encoder := differentialEncoder{
		descriptor: semantic.Schema{Embedding: embedding, Chunking: config.Schema.Chunking},
		vectors:    make(map[fts.DocID][]semantic.EncodedChunk),
	}
	query := fts.Document{ID: "query"}
	encoder.vectors[query.ID] = []semantic.EncodedChunk{differentialChunk("query", "query", 0, []float32{0, 0})}
	reference := make(map[fts.DocID][]semantic.EncodedChunk)

	assertMatches := func() {
		t.Helper()
		got, err := service.SearchDocumentsWithOptions(ctx, encoder, query, 4, semantic.SearchOptions{
			EfSearch: 64, VisitLimit: 64, CandidateChunks: 64,
		})
		if err != nil {
			t.Fatal(err)
		}
		want := exactDocumentHits(t, embedding, encoder.vectors[query.ID][0].Vector, reference, 4, 2)
		assertDocumentHitsEqual(t, want, got.Hits)
	}
	add := func(docID fts.DocID, values ...semantic.EncodedChunk) {
		t.Helper()
		encoder.vectors[docID] = slices.Clone(values)
		if err := service.AddDocument(ctx, encoder, fts.Document{ID: docID}); err != nil {
			t.Fatal(err)
		}
		if err := service.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		reference[docID] = slices.Clone(values)
		assertMatches()
	}
	replace := func(docID fts.DocID, values ...semantic.EncodedChunk) {
		t.Helper()
		encoder.vectors[docID] = slices.Clone(values)
		if err := service.ReplaceDocument(ctx, encoder, fts.Document{ID: docID}); err != nil {
			t.Fatal(err)
		}
		if err := service.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		reference[docID] = slices.Clone(values)
		assertMatches()
	}

	add("doc-a",
		differentialChunk("doc-a", "a-0", 0, []float32{1, 0}),
		differentialChunk("doc-a", "a-1", 1, []float32{1, 1}),
	)
	add("doc-b", differentialChunk("doc-b", "b-0", 0, []float32{2, 0}))
	add("doc-c", differentialChunk("doc-c", "c-0", 0, []float32{3, 0}))
	replace("doc-a", differentialChunk("doc-a", "a-new", 0, []float32{4, 0}))
	if err := service.DeleteDocument(ctx, "doc-b"); err != nil {
		t.Fatal(err)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	delete(reference, "doc-b")
	assertMatches()

	before, err := service.SearchDocumentsWithOptions(ctx, encoder, query, 4, semantic.SearchOptions{EfSearch: 64, VisitLimit: 64, CandidateChunks: 64})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := service.SearchDocumentsWithOptions(ctx, encoder, query, 4, semantic.SearchOptions{EfSearch: 64, VisitLimit: 64, CandidateChunks: 64})
	if err != nil {
		t.Fatal(err)
	}
	assertDocumentHitsEqual(t, before.Hits, after.Hits)
	assertMatches()
}

func exactDocumentHits(t *testing.T, descriptor semantic.EmbeddingDescriptor, query []float32, documents map[fts.DocID][]semantic.EncodedChunk, maxDocuments, maxChunks int) []semantic.DocumentHit {
	t.Helper()
	calculator, err := descriptor.Calculator()
	if err != nil {
		t.Fatal(err)
	}
	preparedQuery, err := calculator.Prepare(query)
	if err != nil {
		t.Fatal(err)
	}
	hits := make([]semantic.DocumentHit, 0, len(documents))
	for docID, values := range documents {
		chunks := make([]semantic.ChunkHit, 0, len(values))
		for _, value := range values {
			prepared, err := calculator.Prepare(value.Vector)
			if err != nil {
				t.Fatal(err)
			}
			chunks = append(chunks, semantic.ChunkHit{Ref: value.Ref, Distance: calculator.DistancePrepared(preparedQuery, prepared)})
		}
		slices.SortFunc(chunks, func(a, b semantic.ChunkHit) int {
			if a.Distance < b.Distance {
				return -1
			}
			if a.Distance > b.Distance {
				return 1
			}
			if a.Ref.ID < b.Ref.ID {
				return -1
			}
			if a.Ref.ID > b.Ref.ID {
				return 1
			}
			return 0
		})
		if len(chunks) > maxChunks {
			chunks = chunks[:maxChunks]
		}
		hits = append(hits, semantic.DocumentHit{DocID: docID, Distance: chunks[0].Distance, Chunks: chunks})
	}
	slices.SortFunc(hits, func(a, b semantic.DocumentHit) int {
		if a.Distance < b.Distance {
			return -1
		}
		if a.Distance > b.Distance {
			return 1
		}
		if a.DocID < b.DocID {
			return -1
		}
		if a.DocID > b.DocID {
			return 1
		}
		return 0
	})
	if len(hits) > maxDocuments {
		hits = hits[:maxDocuments]
	}
	return hits
}

func assertDocumentHitsEqual(t *testing.T, want, got []semantic.DocumentHit) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("hit count = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if want[i].DocID != got[i].DocID || math.Abs(want[i].Distance-got[i].Distance) > 1e-6 || len(want[i].Chunks) != len(got[i].Chunks) {
			t.Fatalf("hit %d mismatch: want=%+v got=%+v", i, want[i], got[i])
		}
		for j := range want[i].Chunks {
			if want[i].Chunks[j].Ref != got[i].Chunks[j].Ref || math.Abs(want[i].Chunks[j].Distance-got[i].Chunks[j].Distance) > 1e-6 {
				t.Fatalf("chunk %d/%d mismatch: want=%+v got=%+v", i, j, want[i].Chunks[j], got[i].Chunks[j])
			}
		}
	}
}

func differentialChunk(docID fts.DocID, id chunk.ID, ordinal uint32, value []float32) semantic.EncodedChunk {
	return semantic.EncodedChunk{
		Ref:    chunk.Ref{ID: id, DocID: docID, Field: fts.DefaultField, Ordinal: ordinal, StartByte: uint64(ordinal), EndByte: uint64(ordinal + 1)},
		Vector: value,
	}
}
