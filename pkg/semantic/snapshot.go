package semantic

import (
	"context"
	"fmt"
	"slices"

	"github.com/dariasmyr/fts-engine/internal/contextcheck"
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
	maxEfSearch             int
	maxVisitLimit           int
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
	if options.EfSearch < 0 || options.EfSearch > v.maxEfSearch ||
		options.VisitLimit < 0 || options.VisitLimit > v.maxVisitLimit {
		return ErrInvalidSearchOptions
	}
	return nil
}

func (v *Snapshot) searchEncodedQueries(ctx context.Context, queries []vector.PreparedQuery, k int, options SearchOptions) (DocumentSearchResult, error) {
	if v.liveCount == 0 {
		return DocumentSearchResult{
			Hits:  []DocumentHit{},
			Stats: hnsw.SearchStats{Termination: hnsw.TerminationComplete},
		}, nil
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
	type chunkKey struct {
		documentID fts.DocID
		chunkID    ChunkID
	}

	result := DocumentSearchResult{Stats: hnsw.SearchStats{Termination: hnsw.TerminationComplete}}
	candidateBudget, _ := resolveCandidateBudget(options.CandidateChunks, v.maxCandidates)
	visitBudget := options.VisitLimit
	if visitBudget == 0 {
		visitBudget = v.searchConfig.VisitLimit
	}
	efSearch := options.EfSearch
	if efSearch == 0 {
		efSearch = v.searchConfig.EfSearch
	}

	active := make([]segmentView, 0, len(v.segments))
	for i, view := range v.segments {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return nil, DocumentSearchResult{}, err
		}
		if view.liveness.AllowedOrdinalCount() > 0 {
			active = append(active, view)
		}
	}
	visitShares := make([][]int, len(queries))
	for queryIndex := range queries {
		visitShares[queryIndex] = make([]int, len(active))
	}
	type searchTask struct {
		queryIndex   int
		segmentIndex int
	}
	tasks := make([]searchTask, 0)
	capacities := make([]int, 0)
	work := 0
	for segmentRound := range active {
		for queryIndex := range queries {
			if err := contextcheck.PeriodicError(ctx, work); err != nil {
				return nil, DocumentSearchResult{}, err
			}
			work++
			segmentIndex := (segmentRound + queryIndex) % len(active)
			tasks = append(tasks, searchTask{
				queryIndex:   queryIndex,
				segmentIndex: segmentIndex,
			})
			capacities = append(capacities, active[segmentIndex].segment.len())
		}
	}
	// Water-fill each task up to its physical segment size, redistributing work
	// that small segments cannot use while preserving diagonal remainder order.
	if err := ctx.Err(); err != nil {
		return nil, DocumentSearchResult{}, err
	}
	slices.Sort(capacities)
	if err := ctx.Err(); err != nil {
		return nil, DocumentSearchResult{}, err
	}
	level, remaining, remainingTasks := 0, visitBudget, len(tasks)
	for i, capacity := range capacities {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return nil, DocumentSearchResult{}, err
		}
		if capacity > level {
			delta := capacity - level
			if delta > remaining/remainingTasks {
				level += remaining / remainingTasks
				remaining %= remainingTasks
				break
			}
			remaining -= delta * remainingTasks
			level = capacity
		}
		remainingTasks--
		if remainingTasks == 0 {
			remaining = 0
			break
		}
	}
	for i, task := range tasks {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return nil, DocumentSearchResult{}, err
		}
		visitShares[task.queryIndex][task.segmentIndex] = min(level, active[task.segmentIndex].segment.len())
	}
	for i, task := range tasks {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return nil, DocumentSearchResult{}, err
		}
		if remaining == 0 {
			break
		}
		if visitShares[task.queryIndex][task.segmentIndex] < active[task.segmentIndex].segment.len() {
			visitShares[task.queryIndex][task.segmentIndex]++
			remaining--
		}
	}

	merged := make(map[chunkKey]ChunkHit, v.liveCount)
	visitLimited := false
	mergedHits := 0
	for queryIndex, query := range queries {
		for segmentIndex, view := range active {
			if err := ctx.Err(); err != nil {
				return nil, DocumentSearchResult{}, err
			}
			share := visitShares[queryIndex][segmentIndex]
			if share == 0 {
				visitLimited = true
				continue
			}
			partial, err := searchSegmentChunksPrepared(ctx, v.calculator, view, query,
				min(candidateBudget, view.liveness.AllowedOrdinalCount()), hnsw.SearchOptions{
					EfSearch: efSearch, VisitLimit: share,
				})
			if err != nil {
				return nil, DocumentSearchResult{}, err
			}
			mergeSearchStats(&result.Stats, partial.Stats)
			visitLimited = visitLimited || partial.Incomplete
			for _, hit := range partial.Hits {
				if err := contextcheck.PeriodicError(ctx, mergedHits); err != nil {
					return nil, DocumentSearchResult{}, err
				}
				mergedHits++
				key := chunkKey{documentID: hit.Ref.DocID, chunkID: hit.Ref.ID}
				previous, exists := merged[key]
				if !exists || compareChunkHits(hit, previous) < 0 {
					merged[key] = hit
				}
			}
		}
	}

	candidates := make([]ChunkHit, 0, len(merged))
	i := 0
	for _, hit := range merged {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return nil, DocumentSearchResult{}, err
		}
		candidates = append(candidates, hit)
		i++
	}
	if err := ctx.Err(); err != nil {
		return nil, DocumentSearchResult{}, err
	}
	slices.SortFunc(candidates, compareChunkHits)
	if err := ctx.Err(); err != nil {
		return nil, DocumentSearchResult{}, err
	}
	candidateLimited := candidateBudget < v.liveCount
	if len(candidates) > candidateBudget {
		candidates = candidates[:candidateBudget]
		candidateLimited = true
	}
	result.CandidateChunks = len(candidates)
	result.GroupingIncomplete = visitLimited || candidateLimited
	switch {
	case visitLimited:
		result.Stats.Termination = hnsw.TerminationVisitLimit
	case candidateLimited:
		result.Stats.Termination = "candidate_limit"
	default:
		result.Stats.Termination = hnsw.TerminationComplete
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
		maxEfSearch: policy.MaxEfSearch, maxVisitLimit: policy.MaxVisitLimit,
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
		chunkID    ChunkID
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

// SearchEncoded searches an immutable snapshot using already
// encoded query chunks.
//
// Chunk references are not used for ranking. Only the prepared
// vectors participate in similarity search.
func (s *Snapshot) SearchEncoded(
	ctx context.Context,
	queries []EncodedChunk,
	k int,
	options SearchOptions,
) (DocumentSearchResult, error) {
	if s == nil {
		return DocumentSearchResult{}, ErrInvalidSegment
	}

	if err := s.validateSearchLimits(ctx, k, options); err != nil {
		return DocumentSearchResult{}, err
	}

	if len(queries) == 0 || len(queries) > s.maxQueryChunks {
		return DocumentSearchResult{}, ErrInvalidQuery
	}

	type preparedInput struct {
		query vector.PreparedQuery
		key   []float32
	}

	inputs := make([]preparedInput, len(queries))

	for n, item := range queries {
		if err := ctx.Err(); err != nil {
			return DocumentSearchResult{}, err
		}

		query, err := s.calculator.PrepareQuery(item.Vector)
		if err != nil {
			return DocumentSearchResult{}, err
		}

		key, err := s.calculator.Prepare(item.Vector)
		if err != nil {
			return DocumentSearchResult{}, err
		}

		inputs[n] = preparedInput{
			query: query,
			key:   key,
		}
	}

	// Preserve deterministic search order independently
	// of the original query chunk ordering.
	slices.SortFunc(inputs, func(a, b preparedInput) int {
		for i := range a.key {
			if a.key[i] < b.key[i] {
				return -1
			}
			if a.key[i] > b.key[i] {
				return 1
			}
		}

		return 0
	})

	if err := ctx.Err(); err != nil {
		return DocumentSearchResult{}, err
	}

	prepared := make([]vector.PreparedQuery, len(inputs))
	for n := range inputs {
		prepared[n] = inputs[n].query
	}

	return s.searchEncodedQueries(ctx, prepared, k, options)
}
