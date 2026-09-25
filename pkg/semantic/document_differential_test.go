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

type deterministicEncoder struct {
	descriptor semantic.PipelineDescriptor
	documents  map[fts.DocID][]semantic.ChunkVector
	queries    map[fts.DocID][]semantic.ChunkVector
}

func (e *deterministicEncoder) Descriptor() semantic.PipelineDescriptor { return e.descriptor }

func (e *deterministicEncoder) Encode(_ context.Context, document semantic.Document) ([]semantic.ChunkVector, error) {
	values := e.documents[document.ID]
	if queryValues, ok := e.queries[document.ID]; ok {
		values = queryValues
	}
	return cloneChunkVectors(values), nil
}

type exactDocumentReference struct {
	calculator vector.Calculator
	documents  map[fts.DocID][]semantic.ChunkVector
}

func TestDocumentSearchDifferentialThroughLifecycle(t *testing.T) {
	ctx := context.Background()
	embedding, err := semantic.NewEmbeddingDescriptor("test", "model", "v1", "pipeline-v1", 2, vector.MetricL2Squared, 1)
	if err != nil {
		t.Fatal(err)
	}
	config := semantic.Config{
		Embedding:               embedding,
		Chunking:                semantic.ChunkingDescriptor{ID: "chunks", Version: 1, Fingerprint: "chunks-v1"},
		MaxVectors:              128,
		MaxChunksPerDocument:    8,
		MaxK:                    5,
		MaxChunkCandidates:      128,
		MaxChunksPerDocumentHit: 2,
	}
	service, err := semantic.New(config)
	if err != nil {
		t.Fatal(err)
	}
	encoder := &deterministicEncoder{
		descriptor: semantic.PipelineDescriptor{Embedding: embedding, Chunking: config.Chunking},
		documents:  make(map[fts.DocID][]semantic.ChunkVector),
		queries:    make(map[fts.DocID][]semantic.ChunkVector),
	}
	query := semantic.Document{ID: "query"}
	encoder.queries[query.ID] = []semantic.ChunkVector{testSemanticChunk("query", "query-0", 0, []float32{0, 0})}
	calculator, err := embedding.Calculator()
	if err != nil {
		t.Fatal(err)
	}
	reference := &exactDocumentReference{calculator: calculator, documents: make(map[fts.DocID][]semantic.ChunkVector)}

	add := func(docID fts.DocID, values ...semantic.ChunkVector) {
		t.Helper()
		encoder.documents[docID] = cloneChunkVectors(values)
		if err := service.AddDocument(ctx, encoder, semantic.Document{ID: docID}); err != nil {
			t.Fatal(err)
		}
		if err := service.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		reference.documents[docID] = cloneChunkVectors(values)
		assertDocumentSearchMatches(t, service, encoder, reference, query)
	}
	replace := func(docID fts.DocID, values ...semantic.ChunkVector) {
		t.Helper()
		encoder.documents[docID] = cloneChunkVectors(values)
		if err := service.ReplaceDocument(ctx, encoder, semantic.Document{ID: docID}); err != nil {
			t.Fatal(err)
		}
		if err := service.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		reference.documents[docID] = cloneChunkVectors(values)
		assertDocumentSearchMatches(t, service, encoder, reference, query)
	}
	remove := func(docID fts.DocID) {
		t.Helper()
		if !service.DeleteDocument(docID) {
			t.Fatalf("DeleteDocument(%q) returned false", docID)
		}
		if err := service.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		delete(reference.documents, docID)
		assertDocumentSearchMatches(t, service, encoder, reference, query)
	}

	for i := range 10 {
		docID := fts.DocID("doc-" + string(rune('a'+i)))
		add(docID,
			testSemanticChunk(docID, chunk.ID(string(docID)+"-0"), 0, []float32{float32(i + 1), 0}),
			testSemanticChunk(docID, chunk.ID(string(docID)+"-1"), 1, []float32{float32(i + 1), 1}),
		)
	}
	for i := 0; i < 6; i++ {
		docID := fts.DocID("doc-" + string(rune('a'+i)))
		replace(docID, testSemanticChunk(docID, chunk.ID(string(docID)+"-replacement"), 0, []float32{float32(20 - i), 0}))
	}
	for i := 6; i < 9; i++ {
		remove(fts.DocID("doc-" + string(rune('a'+i))))
	}
	if stats := service.Statistics(); stats.StaleVectors == 0 {
		t.Fatal("expected stale vectors before compaction")
	}

	before, err := service.SearchDocumentsWithOptions(ctx, encoder, query, config.MaxK, exactSearchOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := service.SearchDocumentsWithOptions(ctx, encoder, query, config.MaxK, exactSearchOptions())
	if err != nil {
		t.Fatal(err)
	}
	assertDocumentResultsEqual(t, before, after)
	if stats := service.Statistics(); stats.StaleVectors != 0 {
		t.Fatalf("stale vectors after compaction = %d", stats.StaleVectors)
	}
	assertDocumentSearchMatches(t, service, encoder, reference, query)
}

func assertDocumentSearchMatches(t *testing.T, service *semantic.Service, encoder semantic.Encoder, reference *exactDocumentReference, query semantic.Document) {
	t.Helper()
	got, err := service.SearchDocumentsWithOptions(context.Background(), encoder, query, 5, exactSearchOptions())
	if err != nil {
		t.Fatal(err)
	}
	want, err := reference.search(encoder, query, 5, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertDocumentResultsEqual(t, want, got)
}

func (r *exactDocumentReference) search(encoder semantic.Encoder, query semantic.Document, k, maxChunks int) (semantic.DocumentSearchResult, error) {
	queries, err := encoder.Encode(context.Background(), query)
	if err != nil {
		return semantic.DocumentSearchResult{}, err
	}
	merged := make(map[fts.DocID]semantic.DocumentHit)
	totalCandidates := 0
	for _, queryChunk := range queries {
		preparedQuery, err := r.calculator.Prepare(queryChunk.Vector)
		if err != nil {
			return semantic.DocumentSearchResult{}, err
		}
		type candidate struct {
			hit semantic.ChunkHit
		}
		candidates := make([]candidate, 0)
		for _, chunks := range r.documents {
			for _, item := range chunks {
				prepared, err := r.calculator.Prepare(item.Vector)
				if err != nil {
					return semantic.DocumentSearchResult{}, err
				}
				candidates = append(candidates, candidate{hit: semantic.ChunkHit{Ref: item.Ref, Distance: r.calculator.DistancePrepared(preparedQuery, prepared)}})
			}
		}
		slices.SortFunc(candidates, func(a, b candidate) int {
			if a.hit.Distance < b.hit.Distance {
				return -1
			}
			if a.hit.Distance > b.hit.Distance {
				return 1
			}
			if a.hit.Ref.DocID < b.hit.Ref.DocID {
				return -1
			}
			if a.hit.Ref.DocID > b.hit.Ref.DocID {
				return 1
			}
			if a.hit.Ref.ID < b.hit.Ref.ID {
				return -1
			}
			if a.hit.Ref.ID > b.hit.Ref.ID {
				return 1
			}
			return 0
		})
		totalCandidates += len(candidates)
		for _, candidate := range candidates {
			group := merged[candidate.hit.Ref.DocID]
			if group.DocID == "" {
				group = semantic.DocumentHit{DocID: candidate.hit.Ref.DocID, Distance: candidate.hit.Distance}
			}
			if len(group.Chunks) < maxChunks {
				group.Chunks = append(group.Chunks, candidate.hit)
			}
			if candidate.hit.Distance < group.Distance {
				group.Distance = candidate.hit.Distance
			}
			merged[candidate.hit.Ref.DocID] = group
		}
	}
	hits := make([]semantic.DocumentHit, 0, len(merged))
	for _, hit := range merged {
		hits = append(hits, hit)
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
	distinct := len(hits)
	if len(hits) > k {
		hits = hits[:k]
	}
	return semantic.DocumentSearchResult{Hits: hits, CandidateChunks: totalCandidates, DistinctDocuments: distinct}, nil
}

func assertDocumentResultsEqual(t *testing.T, want, got semantic.DocumentSearchResult) {
	t.Helper()
	if want.GroupingIncomplete != got.GroupingIncomplete {
		t.Fatalf("grouping incomplete mismatch: want=%v got=%v", want.GroupingIncomplete, got.GroupingIncomplete)
	}
	if len(want.Hits) != len(got.Hits) {
		t.Fatalf("document hit count = %d, want %d: got=%+v", len(got.Hits), len(want.Hits), got.Hits)
	}
	for i := range want.Hits {
		left, right := want.Hits[i], got.Hits[i]
		if left.DocID != right.DocID || math.Abs(left.Distance-right.Distance) > 1e-5 || len(left.Chunks) != len(right.Chunks) {
			t.Fatalf("document hit %d mismatch: want=%+v got=%+v", i, left, right)
		}
		for j := range left.Chunks {
			if left.Chunks[j].Ref != right.Chunks[j].Ref || math.Abs(left.Chunks[j].Distance-right.Chunks[j].Distance) > 1e-5 {
				t.Fatalf("chunk hit %d/%d mismatch: want=%+v got=%+v", i, j, left.Chunks[j], right.Chunks[j])
			}
		}
	}
}

func testSemanticChunk(docID fts.DocID, id chunk.ID, ordinal uint32, value []float32) semantic.ChunkVector {
	return semantic.ChunkVector{Ref: chunk.Ref{ID: id, DocID: docID, Field: fts.DefaultField, Ordinal: ordinal, StartByte: uint64(ordinal * 10), EndByte: uint64(ordinal*10 + 10)}, Vector: value}
}

func cloneChunkVectors(values []semantic.ChunkVector) []semantic.ChunkVector {
	cloned := make([]semantic.ChunkVector, len(values))
	for i, value := range values {
		cloned[i] = value
		cloned[i].Vector = slices.Clone(value.Vector)
	}
	return cloned
}

func exactSearchOptions() semantic.SearchOptions {
	return semantic.SearchOptions{EfSearch: 128, VisitLimit: 128, CandidateChunks: 128}
}
