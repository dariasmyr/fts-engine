package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// Reader provides immutable chunk and grouped-document search over one
// recovered semantic checkpoint.
type Reader struct {
	view       searchView
	checkpoint Checkpoint
}

func OpenCheckpoint(checkpoint Checkpoint) (*Reader, error) {
	if err := checkpoint.Validate(); err != nil {
		return nil, err
	}
	checkpoint = cloneCheckpoint(checkpoint)
	space, _ := vector.NewSpace(checkpoint.Space.Dimensions, checkpoint.Space.Metric)
	refs := make(map[VectorID]chunk.Ref, len(checkpoint.Refs))
	for _, record := range checkpoint.Refs {
		refs[record.VectorID] = record.Ref
	}
	return &Reader{
		view: searchView{
			space: space, searcher: checkpoint.Segment, live: checkpoint.Live,
			vectorIDs: checkpoint.VectorIDs, refs: refs, maxK: checkpoint.MaxK,
			maxChunkCandidates: checkpoint.MaxChunkCandidates, maxChunksPerDocumentHit: checkpoint.MaxChunksPerDocumentHit,
		},
		checkpoint: checkpoint,
	}, nil
}

func (r *Reader) SearchChunks(ctx context.Context, query []float32, k int) (ChunkSearchResult, error) {
	return searchChunks(ctx, r.view, query, k)
}

func (r *Reader) SearchDocuments(ctx context.Context, query []float32, k int) (DocumentSearchResult, error) {
	return searchDocuments(ctx, r.view, query, k)
}

func (r *Reader) Checkpoint() Checkpoint { return cloneCheckpoint(r.checkpoint) }

func (r *Reader) Close() error { return r.checkpoint.Segment.Close() }

func cloneDocumentRecords(records []DocumentRecord) []DocumentRecord {
	cloned := make([]DocumentRecord, len(records))
	for i, record := range records {
		cloned[i] = DocumentRecord{DocID: record.DocID, VectorIDs: append([]VectorID(nil), record.VectorIDs...)}
	}
	return cloned
}

func cloneCheckpoint(checkpoint Checkpoint) Checkpoint {
	checkpoint.VectorIDs = append([]VectorID(nil), checkpoint.VectorIDs...)
	checkpoint.Documents = cloneDocumentRecords(checkpoint.Documents)
	checkpoint.Refs = append([]RefRecord(nil), checkpoint.Refs...)
	if checkpoint.DuplicateStatistics != nil {
		stats := *checkpoint.DuplicateStatistics
		checkpoint.DuplicateStatistics = &stats
	}
	return checkpoint
}
