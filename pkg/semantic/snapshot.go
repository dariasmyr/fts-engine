package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

// Snapshot is one immutable committed semantic snapshot. It owns neither
// mutable ingest state nor persistence resources, so old views remain usable
// while the service publishes newer views.
type Snapshot struct {
	segments                []segmentView
	revision                Revision
	liveCount               int
	schema                  Schema
	maxDocumentsPerSearch   int
	maxCandidates           int
	maxChunksPerDocumentHit int
	maxQueryChunks          int
	maxEfSearch             int
	maxVisitLimit           int
	searchConfig            hnsw.SearchConfig
	calculator              vector.Calculator
}

func newSnapshot(
	ctx context.Context,
	revision Revision,
	segments []segmentView,
	schema Schema,
	policy searchPolicy,
	search hnsw.SearchConfig,
) (*Snapshot, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !schema.IsValid() || policy.validate() != nil {
		return nil, ErrInvalidConfig
	}
	calculator, err := schema.Embedding.Calculator()
	if err != nil {
		return nil, ErrInvalidConfig
	}
	if err := validateSegmentViews(ctx, segments, schema, search); err != nil {
		return nil, err
	}
	return newTrustedSnapshot(ctx, revision, segments, schema, policy, search, calculator)
}

// newTrustedSnapshot publishes segments already validated at ingestion or
// compaction boundaries. It validates only snapshot-local shape while preserving
// full historical validation for New and Restore through newSnapshot.
func newTrustedSnapshot(
	ctx context.Context,
	revision Revision,
	segments []segmentView,
	schema Schema,
	policy searchPolicy,
	search hnsw.SearchConfig,
	calculator vector.Calculator,
) (*Snapshot, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	view := &Snapshot{
		segments:                segments,
		revision:                revision,
		schema:                  schema,
		maxDocumentsPerSearch:   policy.MaxDocumentsPerSearch,
		maxCandidates:           policy.MaxChunkCandidates,
		maxChunksPerDocumentHit: policy.MaxChunksPerDocumentHit,
		maxQueryChunks:          policy.MaxQueryChunks,
		maxEfSearch:             policy.MaxEfSearch,
		maxVisitLimit:           policy.MaxVisitLimit,
		searchConfig:            search,
		calculator:              calculator,
	}
	for i, segment := range view.segments {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return nil, err
		}
		if segment.segment == nil || segment.liveness.TotalOrdinalCount() != uint32(segment.segment.len()) {
			return nil, ErrInternalState
		}
		view.liveCount += segment.liveness.AllowedOrdinalCount()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return view, nil
}
