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
	segments                []segmentView
	generation              uint64
	liveCount               int
	maxK                    int
	maxCandidates           int
	maxChunksPerDocumentHit int
	vectorSpace             vector.Space
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
		partial, err := searchReadViewDocuments(ctx, v, v.vectorSpace, maxResults, maxCandidates, maxChunks, item.Vector, k, options)
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

// NewReadView creates a coherent immutable view over fully live segments.
// Service-owned views may additionally carry component-local tombstone filters.
func NewReadView(generation uint64, segments []*Segment) (*ReadView, error) {
	return fullReadView(generation, segments)
}

func newReadView(generation uint64, segments []segmentView) *ReadView {
	view := &ReadView{
		segments:   append([]segmentView(nil), segments...),
		generation: generation,
	}
	for _, segment := range view.segments {
		if segment.segment != nil {
			view.maxK = max(view.maxK, segment.segment.MaxK())
			if view.vectorSpace.Dimensions() == 0 {
				view.vectorSpace, _ = vector.NewSpace(segment.segment.Dimensions(), segment.segment.Metric())
			}
		}
	}
	view.maxCandidates = view.maxK
	view.maxChunksPerDocumentHit = 1
	for _, segment := range view.segments {
		if segment.filter.TotalOrdinalCount() > 0 {
			view.liveCount += segment.filter.AllowedOrdinalCount()
		}
	}
	return view
}

func fullReadView(generation uint64, segments []*Segment) (*ReadView, error) {
	views := make([]segmentView, 0, len(segments))
	for _, segment := range segments {
		if segment == nil {
			return nil, ErrInvalidSegment
		}
		filter := vector.NewFullBitSet(uint32(segment.Len()))
		views = append(views, segmentView{segment: segment, filter: filter})
	}
	return newReadView(generation, views), nil
}
