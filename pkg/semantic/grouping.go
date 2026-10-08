package semantic

import (
	"context"
	"slices"

	"github.com/dariasmyr/fts-engine/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type documentAccumulator struct {
	distance float64
	chunks   map[chunk.ID]ChunkHit
}

// groupDocumentHits is the semantic grouping layer. It deduplicates chunk
// identities across query chunks, groups candidates by document and applies the
// document-level result limits independently of ANN retrieval.
func groupDocumentHits(ctx context.Context, candidates []ChunkHit, k, maxChunksPerDocument int) ([]DocumentHit, int, error) {
	if ctx == nil {
		return nil, 0, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	merged := make(map[fts.DocID]*documentAccumulator)
	for n, hit := range candidates {
		if err := contextcheck.PeriodicError(ctx, n); err != nil {
			return nil, 0, err
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
		if err := contextcheck.PeriodicError(ctx, n); err != nil {
			return nil, 0, err
		}
		chunks := make([]ChunkHit, 0, len(accumulated.chunks))
		chunkIndex := 0
		for _, hit := range accumulated.chunks {
			if err := contextcheck.PeriodicError(ctx, chunkIndex); err != nil {
				return nil, 0, err
			}
			chunks = append(chunks, hit)
			chunkIndex++
		}
		slices.SortFunc(chunks, compareChunkHits)
		if len(chunks) > maxChunksPerDocument {
			chunks = chunks[:maxChunksPerDocument]
		}
		hits = append(hits, DocumentHit{DocID: docID, Distance: accumulated.distance, Chunks: chunks})
		n++
	}

	if err := ctx.Err(); err != nil {
		return nil, 0, err
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
