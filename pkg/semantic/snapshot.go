package semantic

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
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
	searchConfig            hnsw.SearchConfig
	calculator              vector.Calculator
}

func (v *Snapshot) validateSearchLimits(ctx context.Context, maxResultCount int, options SearchOptions) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if maxResultCount <= 0 || maxResultCount > v.maxDocumentsPerSearch {
		return fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, maxResultCount, v.maxDocumentsPerSearch)
	}
	if _, err := resolveCandidateBudget(options.CandidateChunks, v.maxCandidates); err != nil {
		return err
	}
	if options.EfSearch < 0 || options.VisitLimit < 0 {
		return ErrInvalidSearchOptions
	}
	return nil
}

func (v *Snapshot) searchEncodedQueries(ctx context.Context, queries []vector.PreparedQuery, k int, options SearchOptions) (DocumentSearchResult, error) {
	if v.liveCount == 0 {
		return DocumentSearchResult{Hits: []DocumentHit{}}, nil
	}

	candidates, result, err := v.collectChunkCandidates(ctx, queries, options)
	if err != nil {
		return DocumentSearchResult{}, err
	}

	result.Hits, result.DistinctDocuments, err = groupDocumentHits(
		ctx,
		candidates,
		k,
		v.maxChunksPerDocumentHit,
	)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	return result, nil
}

// collectChunkCandidates is the ANN retrieval layer. It knows nothing about
// document grouping: it only spends request-wide ANN budgets and returns chunk hits.
func (v *Snapshot) collectChunkCandidates(ctx context.Context, queries []vector.PreparedQuery, options SearchOptions) ([]ChunkHit, DocumentSearchResult, error) {
	var candidates []ChunkHit
	var result DocumentSearchResult
	candidateBudget, _ := resolveCandidateBudget(options.CandidateChunks, v.maxCandidates)
	visitBudget := options.VisitLimit
	if visitBudget == 0 {
		visitBudget = v.searchConfig.VisitLimit
	}

	for _, query := range queries {
		if err := ctx.Err(); err != nil {
			return nil, DocumentSearchResult{}, err
		}

		remainingCandidates := candidateBudget - result.CandidateChunks
		remainingVisits := visitBudget - result.Stats.VisitedNodes
		if remainingCandidates <= 0 || remainingVisits <= 0 {
			result.GroupingIncomplete = true
			break
		}

		budget := min(v.liveCount, remainingCandidates)
		partial, err := searchSegmentsChunksPrepared(ctx, v.calculator, v.segments, query, budget, candidateBudget, hnsw.SearchOptions{
			EfSearch:   options.EfSearch,
			VisitLimit: remainingVisits,
		})
		if err != nil {
			return nil, DocumentSearchResult{}, err
		}

		candidates = append(candidates, partial.Hits...)
		result.CandidateChunks += len(partial.Hits)
		mergeSearchStats(&result.Stats, partial.Stats)
		result.GroupingIncomplete = result.GroupingIncomplete || budget < v.liveCount || partial.Incomplete
	}
	return candidates, result, nil
}

func newSnapshot(ctx context.Context, revision Revision, segments []segmentView, schema Schema, policy searchPolicy, search hnsw.SearchConfig) (*Snapshot, error) {
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
func newTrustedSnapshot(ctx context.Context, revision Revision, segments []segmentView, schema Schema, policy searchPolicy, search hnsw.SearchConfig, calculator vector.Calculator) (*Snapshot, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	view := &Snapshot{
		segments: segments, revision: revision,
		schema: schema, maxDocumentsPerSearch: policy.MaxDocumentsPerSearch, maxCandidates: policy.MaxChunkCandidates,
		maxChunksPerDocumentHit: policy.MaxChunksPerDocumentHit, maxQueryChunks: policy.MaxQueryChunks,
		searchConfig: search, calculator: calculator,
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

func validateSegmentViews(ctx context.Context, segments []segmentView, schema Schema, search hnsw.SearchConfig) error {
	components := make(map[SegmentID]struct{}, len(segments))
	type chunkKey struct {
		documentID fts.DocID
		chunkID    chunk.ID
	}
	liveChunks := make(map[chunkKey]struct{})
	liveDocumentComponents := make(map[fts.DocID]SegmentID)
	var previousVectorID VectorID
	for segmentIndex, item := range segments {
		if segmentIndex%16 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if item.segment == nil || item.liveness.TotalOrdinalCount() != uint32(item.segment.len()) {
			return ErrInvalidSegment
		}
		got := item.segment.schema
		if err := validateSchemaCompatibility(got, schema); err != nil {
			return err
		}
		component := item.segment.componentID()
		if _, exists := components[component]; exists {
			return ErrInvalidSegment
		}
		components[component] = struct{}{}
		for ordinal, row := range item.segment.rows {
			if err := contextcheck.PeriodicError(ctx, ordinal); err != nil {
				return err
			}

			if previousVectorID >= row.VectorID {
				return ErrInvalidSegment
			}
			previousVectorID = row.VectorID
			if !item.liveness.Allows(vector.Ordinal(ordinal)) {
				continue
			}
			if owner, exists := liveDocumentComponents[row.Chunk.DocID]; exists && owner != component {
				return ErrInvalidSegment
			}
			liveDocumentComponents[row.Chunk.DocID] = component
			key := chunkKey{documentID: row.Chunk.DocID, chunkID: row.Chunk.ID}
			if _, exists := liveChunks[key]; exists {
				return ErrInvalidSegment
			}
			liveChunks[key] = struct{}{}
		}
	}
	return nil
}
