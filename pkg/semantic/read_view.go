package semantic

import (
	"context"
	"fmt"
	"slices"

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
	generation              uint64
	liveCount               int
	descriptor              PipelineDescriptor
	maxK                    int
	maxCandidates           int
	maxChunksPerDocumentHit int
	search                  hnsw.SearchConfig
	calculator              vector.Calculator
}

// SearchDocumentsWithOptions is the single document-search implementation used
// by both snapshots and Service.
func (v *ReadView) SearchDocumentsWithOptions(ctx context.Context, encoder Encoder, query Document, k int, options SearchOptions) (DocumentSearchResult, error) {
	if err := v.validateSearchRequest(ctx, encoder, k, options); err != nil {
		return DocumentSearchResult{}, err
	}
	queries, err := encoder.Encode(ctx, query)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return DocumentSearchResult{}, err
	}
	if len(queries) == 0 {
		return DocumentSearchResult{}, ErrInvalidQuery
	}
	for i, item := range queries {
		if i%64 == 0 {
			if err := ctx.Err(); err != nil {
				return DocumentSearchResult{}, err
			}
		}
		if err := v.calculator.Validate(item.Vector); err != nil {
			return DocumentSearchResult{}, err
		}
	}
	return v.searchEncodedQueries(ctx, queries, k, options)
}

func (v *ReadView) validateSearchRequest(ctx context.Context, encoder Encoder, k int, options SearchOptions) error {
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
	if _, err := descriptorsEqual(encoder.Descriptor(), v.descriptor); err != nil {
		return err
	}
	return v.validateSearchLimits(ctx, k, options)
}

func (v *ReadView) validateSearchLimits(ctx context.Context, k int, options SearchOptions) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if k <= 0 || k > v.maxK {
		return fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, v.maxK)
	}
	if _, err := resolveCandidateBudget(options.CandidateChunks, v.maxCandidates); err != nil {
		return err
	}
	if options.EfSearch < 0 || options.VisitLimit < 0 ||
		options.EfSearch > v.search.MaxEfSearch || options.VisitLimit > v.search.MaxVisitLimit {
		return ErrInvalidSearchOptions
	}
	return nil
}

func (v *ReadView) validateEncodedQuery(ctx context.Context, query []float32, k int, options SearchOptions) error {
	if v == nil {
		return ErrInvalidSegment
	}
	if err := v.validateSearchLimits(ctx, k, options); err != nil {
		return err
	}
	return v.calculator.Validate(query)
}

func (v *ReadView) searchEncodedQueries(ctx context.Context, queries []ChunkVector, k int, options SearchOptions) (DocumentSearchResult, error) {
	merged := make(map[fts.DocID]DocumentHit)
	var result DocumentSearchResult
	for _, item := range queries {
		if err := ctx.Err(); err != nil {
			return DocumentSearchResult{}, err
		}
		partial, err := searchReadViewDocuments(ctx, v, item.Vector, options)
		if err != nil {
			return DocumentSearchResult{}, err
		}
		result.CandidateChunks += partial.CandidateChunks
		mergeSearchStats(&result.Stats, partial.Stats)
		result.GroupingIncomplete = result.GroupingIncomplete || partial.GroupingIncomplete
		for i, hit := range partial.Hits {
			if i%256 == 0 {
				if err := ctx.Err(); err != nil {
					return DocumentSearchResult{}, err
				}
			}
			current, exists := merged[hit.DocID]
			if !exists || hit.Distance < current.Distance {
				merged[hit.DocID] = hit
			}
		}
	}
	result.Hits = make([]DocumentHit, 0, len(merged))
	i := 0
	for _, hit := range merged {
		if i%256 == 0 {
			if err := ctx.Err(); err != nil {
				return DocumentSearchResult{}, err
			}
		}
		result.Hits = append(result.Hits, hit)
		i++
	}
	slices.SortFunc(result.Hits, func(a, b DocumentHit) int {
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
	result.DistinctDocuments = len(result.Hits)
	if len(result.Hits) > k {
		result.Hits = result.Hits[:k]
	}
	return result, nil
}

func newReadView(ctx context.Context, generation uint64, segments []visibleSegment, descriptor PipelineDescriptor, policy SearchPolicy, search hnsw.SearchConfig) (*ReadView, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !descriptor.Embedding.IsValid() || !descriptor.Chunking.IsValid() || policy.Validate() != nil ||
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
	view := &ReadView{
		segments: append([]visibleSegment(nil), segments...), generation: generation,
		descriptor: descriptor, maxK: policy.MaxK, maxCandidates: policy.MaxChunkCandidates,
		maxChunksPerDocumentHit: policy.MaxChunksPerDocumentHit, search: search, calculator: calculator,
	}
	for _, segment := range view.segments {
		view.liveCount += segment.filter.AllowedOrdinalCount()
	}
	return view, nil
}

func newEmptyReadView(ctx context.Context, descriptor PipelineDescriptor, policy SearchPolicy, search hnsw.SearchConfig) (*ReadView, error) {
	return newReadView(ctx, 0, nil, descriptor, policy, search)
}

func validateVisibleSegments(ctx context.Context, segments []visibleSegment, descriptor PipelineDescriptor, search hnsw.SearchConfig) error {
	components := make(map[ComponentID]struct{}, len(segments))
	vectorIDs := make(map[VectorID]struct{})
	type chunkKey struct {
		documentID fts.DocID
		chunkID    chunk.ID
	}
	liveChunks := make(map[chunkKey]struct{})
	liveDocumentComponents := make(map[fts.DocID]ComponentID)
	for segmentIndex, item := range segments {
		if segmentIndex%16 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if item.segment == nil || item.filter.TotalOrdinalCount() != uint32(item.segment.Len()) || item.segment.SearchLimits() != search {
			return ErrInvalidSegment
		}
		if err := item.segment.validateContents(); err != nil {
			return err
		}
		got := PipelineDescriptor{Embedding: item.segment.metadata.Embedding, Chunking: item.segment.metadata.Chunking}
		if _, err := descriptorsEqual(got, descriptor); err != nil {
			return err
		}
		component := item.segment.ComponentID()
		if _, exists := components[component]; exists {
			return ErrInvalidSegment
		}
		components[component] = struct{}{}
		for ordinal, row := range item.segment.rows {
			if ordinal%64 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if row.VectorID == 0 || row.Chunk.ID == "" || row.Chunk.DocID == "" || row.Chunk.Field == "" || row.Chunk.StartByte > row.Chunk.EndByte ||
				ordinal > 0 && item.segment.rows[ordinal-1].VectorID >= row.VectorID {
				return ErrInvalidSegment
			}
			if _, exists := vectorIDs[row.VectorID]; exists {
				return ErrInvalidSegment
			}
			vectorIDs[row.VectorID] = struct{}{}
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
