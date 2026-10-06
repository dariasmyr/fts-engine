package semantic

import (
	"context"
	"fmt"
	"slices"

	"github.com/dariasmyr/fts-engine/internal/vector/contextcheck"
	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

// ReadView is one immutable committed semantic snapshot. It owns neither
// mutable ingest state nor persistence resources, so old views remain usable
// while the service publishes newer views.
type ReadView struct {
	segments                []visibleSegment
	revision                uint64
	liveCount               int
	descriptor              PipelineDescriptor
	maxDocumentsPerSearch   int
	maxCandidates           int
	maxChunksPerDocumentHit int
	maxQueryChunks          int
	searchConfig            hnsw.SearchConfig
	calculator              vector.Calculator
}

// SearchDocumentsWithOptions is the single document-search implementation used
// by both snapshots and Service.
func (v *ReadView) SearchDocumentsWithOptions(ctx context.Context, encoder Encoder, query fts.Document, maxResultCount int, options SearchOptions) (DocumentSearchResult, error) {
	if err := v.validateSearchRequest(ctx, encoder, maxResultCount, options); err != nil {
		return DocumentSearchResult{}, err
	}
	queries, err := encoder.Encode(ctx, query)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return DocumentSearchResult{}, err
	}
	if len(queries) == 0 || len(queries) > v.maxQueryChunks {
		return DocumentSearchResult{}, ErrInvalidQuery
	}
	preparedQueries := make([]vector.PreparedQuery, len(queries))
	for i, item := range queries {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return DocumentSearchResult{}, err
		}

		prepared, err := v.calculator.PrepareQuery(item.Vector)
		if err != nil {
			return DocumentSearchResult{}, err
		}
		preparedQueries[i] = prepared
	}
	return v.searchEncodedQueries(ctx, preparedQueries, maxResultCount, options)
}

func (v *ReadView) validateSearchRequest(ctx context.Context, encoder Encoder, maxResultCount int, options SearchOptions) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if v == nil {
		return ErrInvalidSegment
	}
	if encoder == nil {
		return ErrInvalidConfig
	}
	if err := validatePipelineCompatibility(encoder.Descriptor(), v.descriptor); err != nil {
		return err
	}
	return v.validateSearchLimits(ctx, maxResultCount, options)
}

func (v *ReadView) validateSearchLimits(ctx context.Context, maxResultCount int, options SearchOptions) error {
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
	if options.EfSearch < 0 || options.VisitLimit < 0 ||
		options.EfSearch > v.searchConfig.MaxEfSearch || options.VisitLimit > v.searchConfig.MaxVisitLimit {
		return ErrInvalidSearchOptions
	}
	return nil
}

func (v *ReadView) searchEncodedQueries(ctx context.Context, queries []vector.PreparedQuery, k int, options SearchOptions) (DocumentSearchResult, error) {
	type documentAccumulator struct {
		distance float64
		chunks   map[chunk.ID]ChunkHit
	}
	merged := make(map[fts.DocID]*documentAccumulator)
	var result DocumentSearchResult
	if v.liveCount == 0 {
		result.Hits = []DocumentHit{}
		return result, nil
	}
	candidateBudget, _ := resolveCandidateBudget(options.CandidateChunks, v.maxCandidates)
	visitBudget := options.VisitLimit
	if visitBudget == 0 {
		visitBudget = v.searchConfig.DefaultVisitLimit
	}
	for _, item := range queries {
		if err := ctx.Err(); err != nil {
			return DocumentSearchResult{}, err
		}
		remainingCandidates := candidateBudget - result.CandidateChunks
		remainingVisits := visitBudget - result.Stats.VisitedNodes
		if remainingCandidates <= 0 || remainingVisits <= 0 {
			result.GroupingIncomplete = true
			break
		}
		budget := min(v.liveCount, remainingCandidates)
		partial, err := searchSegmentsChunksPrepared(ctx, v.calculator, v.segments, item, budget, candidateBudget, vector.SearchOptions{
			EfSearch: options.EfSearch, VisitLimit: remainingVisits,
		})
		if err != nil {
			return DocumentSearchResult{}, err
		}
		result.CandidateChunks += len(partial.Hits)
		mergeSearchStats(&result.Stats, partial.Stats)
		result.GroupingIncomplete = result.GroupingIncomplete || budget < v.liveCount || partial.Incomplete
		for i, hit := range partial.Hits {
			if i%256 == 0 {
				if err := ctx.Err(); err != nil {
					return DocumentSearchResult{}, err
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
	}
	result.Hits = make([]DocumentHit, 0, len(merged))
	i := 0
	for docID, accumulated := range merged {
		if i%256 == 0 {
			if err := ctx.Err(); err != nil {
				return DocumentSearchResult{}, err
			}
		}
		chunks := make([]ChunkHit, 0, len(accumulated.chunks))
		for _, hit := range accumulated.chunks {
			chunks = append(chunks, hit)
		}
		slices.SortFunc(chunks, compareChunkHits)
		if len(chunks) > v.maxChunksPerDocumentHit {
			chunks = chunks[:v.maxChunksPerDocumentHit]
		}
		result.Hits = append(result.Hits, DocumentHit{DocID: docID, Distance: accumulated.distance, Chunks: chunks})
		i++
	}
	slices.SortFunc(result.Hits, compareDocumentHits)
	result.DistinctDocuments = len(result.Hits)
	if len(result.Hits) > k {
		result.Hits = result.Hits[:k]
	}
	if err := ctx.Err(); err != nil {
		return DocumentSearchResult{}, err
	}
	return result, nil
}

func newReadView(ctx context.Context, revision uint64, segments []visibleSegment, descriptor PipelineDescriptor, policy searchPolicy, search hnsw.SearchConfig) (*ReadView, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !descriptor.Embedding.IsValid() || !descriptor.Chunking.IsValid() || policy.validate() != nil ||
		search.MaxK < policy.MaxChunkCandidates || search.MaxEfSearch < policy.MaxChunkCandidates || search.MaxVisitLimit <= 0 {
		return nil, ErrInvalidConfig
	}
	calculator, err := descriptor.Embedding.Calculator()
	if err != nil {
		return nil, ErrInvalidConfig
	}
	if err := validateVisibleSegments(ctx, segments, descriptor, search); err != nil {
		return nil, err
	}
	return newTrustedReadView(ctx, revision, segments, descriptor, policy, search, calculator)
}

// newTrustedReadView publishes segments already validated at ingestion or
// compaction boundaries. It validates only view-local shape while preserving
// full historical validation for New and Restore through newReadView.
func newTrustedReadView(ctx context.Context, revision uint64, segments []visibleSegment, descriptor PipelineDescriptor, policy searchPolicy, search hnsw.SearchConfig, calculator vector.Calculator) (*ReadView, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	view := &ReadView{
		segments: segments, revision: revision,
		descriptor: descriptor, maxDocumentsPerSearch: policy.MaxDocumentsPerSearch, maxCandidates: policy.MaxChunkCandidates,
		maxChunksPerDocumentHit: policy.MaxChunksPerDocumentHit, maxQueryChunks: policy.MaxQueryChunks,
		searchConfig: search, calculator: calculator,
	}
	for i, segment := range view.segments {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return nil, err
		}

		if segment.segment == nil || segment.filter.TotalOrdinalCount() != uint32(segment.segment.len()) {
			return nil, ErrInternalState
		}
		view.liveCount += segment.filter.AllowedOrdinalCount()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return view, nil
}

func validateVisibleSegments(ctx context.Context, segments []visibleSegment, descriptor PipelineDescriptor, search hnsw.SearchConfig) error {
	components := make(map[uint64]struct{}, len(segments))
	type chunkKey struct {
		documentID fts.DocID
		chunkID    chunk.ID
	}
	liveChunks := make(map[chunkKey]struct{})
	liveDocumentComponents := make(map[fts.DocID]uint64)
	var previousVectorID uint64
	for segmentIndex, item := range segments {
		if segmentIndex%16 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if item.segment == nil || item.filter.TotalOrdinalCount() != uint32(item.segment.len()) || item.segment.searchConfig() != search {
			return ErrInvalidSegment
		}
		got := item.segment.descriptor
		if err := validatePipelineCompatibility(got, descriptor); err != nil {
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
			if !item.filter.Allows(vector.Ordinal(ordinal)) {
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
