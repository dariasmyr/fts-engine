package semantic

import (
	"context"
	"slices"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
)

type documentAccumulator struct {
	distance float64
	chunks   map[chunk.ID]ChunkHit
}

// groupDocumentHits is the semantic grouping layer. It deduplicates chunk
// identities across query chunks, groups candidates by document and applies the
// document-level result limits independently of ANN retrieval.
func groupDocumentHits(ctx context.Context, candidates []ChunkHit, k, maxChunksPerDocument int) ([]DocumentHit, int, error) {
	merged := make(map[fts.DocID]*documentAccumulator)
	for n, hit := range candidates {
		if n%256 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, 0, err
			}
		}
		current := merged[hit.Ref.DocID]
		if current == nil {
			current = &documentAccumulator{distance: hit.Distance, chunks: make(map[chunk.ID]ChunkHit)}
			merged[hit.Ref.DocID] = current
		}
		current.distance = min(current.distance, hit.Distance)
		previous, exists := current.chunks[hit.Ref.ID]
		if !exists || compareChunkHits(hit, previous) < 0 {
			current.chunks[hit.Ref.ID] = hit
		}
	}

	hits := make([]DocumentHit, 0, len(merged))
	n := 0
	for docID, accumulated := range merged {
		if n%256 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, 0, err
			}
		}
		chunks := make([]ChunkHit, 0, len(accumulated.chunks))
		for _, hit := range accumulated.chunks {
			chunks = append(chunks, hit)
		}
		slices.SortFunc(chunks, compareChunkHits)
		if len(chunks) > maxChunksPerDocument {
			chunks = chunks[:maxChunksPerDocument]
		}
		hits = append(hits, DocumentHit{DocID: docID, Distance: accumulated.distance, Chunks: chunks})
		n++
	}

	slices.SortFunc(hits, compareDocumentHits)
	if len(hits) > k {
		hits = hits[:k]
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	return hits, len(merged), nil
}
