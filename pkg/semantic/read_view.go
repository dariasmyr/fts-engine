package semantic

import (
	"context"
	"slices"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// ReadView is the immutable in-memory view searched by a semantic service.
// It owns neither the mutable ingest state nor persistence files.
//
// The view contains component-local liveness filters. A segment remains
// immutable while a new view is published, so readers may continue using an
// older view during a flush or compaction.
type ReadView struct {
	segments                []visibleSegment
	generation              uint64
	liveCount               int
	maxK                    int
	maxCandidates           int
	maxChunksPerDocumentHit int
	calculator              vector.Calculator
}

// Generation returns the publication version represented by the view.
func (v *ReadView) Generation() uint64 {
	if v == nil {
		return 0
	}
	return v.generation
}

// SegmentCount returns the number of visible physical segments.
func (v *ReadView) SegmentCount() int {
	if v == nil {
		return 0
	}
	return len(v.segments)
}

// LiveVectorCount returns the number of live rows visible in the view.
func (v *ReadView) LiveVectorCount() int {
	if v == nil {
		return 0
	}
	return v.liveCount
}

// Segments returns the immutable physical components in publication order.
// The returned slice is a copy; the segments themselves are immutable.
func (v *ReadView) Segments() []*Segment {
	if v == nil {
		return nil
	}
	result := make([]*Segment, len(v.segments))
	for i, item := range v.segments {
		result[i] = item.segment
	}
	return result
}

// SearchDocuments searches this coherent view and groups chunk hits by
// document using the immutable published components.
func (v *ReadView) SearchDocuments(ctx context.Context, encoder Encoder, query Document, k int) (DocumentSearchResult, error) {
	return v.SearchDocumentsWithOptions(ctx, encoder, query, k, SearchOptions{})
}

// SearchDocumentsWithOptions searches this coherent view with request-local
// ANN limits.
func (v *ReadView) SearchDocumentsWithOptions(ctx context.Context, encoder Encoder, query Document, k int, options SearchOptions) (DocumentSearchResult, error) {
	if v == nil || encoder == nil {
		return DocumentSearchResult{}, ErrInvalidSegment
	}
	queries, err := encoder.Encode(ctx, query)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	maxResults := v.maxK
	maxCandidates := v.maxCandidates
	maxChunks := v.maxChunksPerDocumentHit
	if maxResults == 0 {
		maxResults = max(1, k)
	}
	if maxCandidates == 0 {
		maxCandidates = maxResults
	}
	if maxChunks == 0 {
		maxChunks = 1
	}
	merged := make(map[fts.DocID]DocumentHit)
	var result DocumentSearchResult
	for _, item := range queries {
		partial, err := searchReadViewDocuments(ctx, v, v.calculator, maxResults, maxCandidates, maxChunks, item.Vector, k, options)
		if err != nil {
			return DocumentSearchResult{}, err
		}
		result.CandidateChunks += partial.CandidateChunks
		mergeSearchStats(&result.Stats, partial.Stats)
		result.GroupingIncomplete = result.GroupingIncomplete || partial.GroupingIncomplete
		for _, hit := range partial.Hits {
			current, exists := merged[hit.DocID]
			if !exists || hit.Distance < current.Distance {
				merged[hit.DocID] = hit
			}
		}
	}
	result.Hits = make([]DocumentHit, 0, len(merged))
	for _, hit := range merged {
		result.Hits = append(result.Hits, hit)
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

// Close releases resources owned by the view's immutable segments.
func (v *ReadView) Close() error {
	if v == nil {
		return nil
	}
	for _, view := range v.segments {
		if view.segment != nil {
			if err := view.segment.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

// NewReadView creates a coherent immutable view over fully live segments using
// the supplied immutable search and grouping policy. It creates a full
// liveness filter for every segment; service-owned views may additionally carry
// filters that exclude replaced or deleted rows.
func NewReadView(generation uint64, segments []*Segment, policy SearchPolicy) (*ReadView, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return fullReadView(generation, segments, policy)
}

// newReadView assembles a view from segments whose local liveness filters have
// already been prepared by the service.
func newReadView(generation uint64, segments []visibleSegment, policy SearchPolicy) *ReadView {
	view := &ReadView{
		segments:                append([]visibleSegment(nil), segments...),
		generation:              generation,
		maxK:                    policy.MaxK,
		maxCandidates:           policy.MaxChunkCandidates,
		maxChunksPerDocumentHit: policy.MaxChunksPerDocumentHit,
	}
	for _, segment := range view.segments {
		if segment.segment != nil {
			if view.calculator.Dimensions() == 0 {
				view.calculator, _ = vector.NewCalculator(segment.segment.Dimensions(), segment.segment.Metric())
			}
		}
	}
	for _, segment := range view.segments {
		if segment.filter.TotalOrdinalCount() > 0 {
			view.liveCount += segment.filter.AllowedOrdinalCount()
		}
	}
	return view
}

// newEmptyReadView creates the initial published view before the first flush.
func newEmptyReadView(policy SearchPolicy) *ReadView {
	return newReadView(0, []visibleSegment{}, policy)
}

func fullReadView(generation uint64, segments []*Segment, policy SearchPolicy) (*ReadView, error) {
	if err := validateSegmentSet(segments); err != nil {
		return nil, err
	}
	views := make([]visibleSegment, 0, len(segments))
	for _, segment := range segments {
		if segment == nil {
			return nil, ErrInvalidSegment
		}
		filter := vector.NewFullBitSet(uint32(segment.Len()))
		views = append(views, visibleSegment{segment: segment, filter: filter})
	}
	return newReadView(generation, views, policy), nil
}

func validateSegmentSet(segments []*Segment) error {
	if len(segments) == 0 {
		return nil
	}
	if segments[0] == nil {
		return ErrInvalidSegment
	}
	if err := segments[0].Validate(); err != nil {
		return err
	}
	want := PipelineDescriptor{Embedding: segments[0].metadata.Embedding, Chunking: segments[0].metadata.Chunking}
	for _, segment := range segments[1:] {
		if segment == nil {
			return ErrInvalidSegment
		}
		if err := segment.Validate(); err != nil {
			return err
		}
		got := PipelineDescriptor{Embedding: segment.metadata.Embedding, Chunking: segment.metadata.Chunking}
		if _, err := descriptorsEqual(got, want); err != nil {
			return err
		}
	}
	return nil
}
