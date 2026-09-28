package vector

import "context"

const (
	TerminationComplete   = "complete"
	TerminationVisitLimit = "visit_limit"
)

// Hit is one vector-search result. Hits are ordered by ascending Distance and
// then ascending Ordinal.
type Hit struct {
	Ordinal  Ordinal
	Distance float64
}

// ResultFilter is an immutable snapshot controlling which local ordinals may
// appear in search hits. TotalOrdinalCount must match the index's Len.
type ResultFilter interface {
	Allows(Ordinal) bool
	AllowedOrdinalCount() int
	TotalOrdinalCount() uint32
}

// SearchOptions controls request-local HNSW work and result filtering.
type SearchOptions struct {
	EfSearch     int
	VisitLimit   int
	ResultFilter ResultFilter
}

// SearchStats describes the work performed for one search request.
type SearchStats struct {
	VisitedNodes         int
	ExpandedNodes        int
	DistanceComputations int
	RejectedNodes        int
	Termination          string
}

// SearchResult contains ordered hits and request execution details.
type SearchResult struct {
	Hits       []Hit
	Stats      SearchStats
	Incomplete bool
}

// Index is implemented by mutable and immutable vector indexes.
type Index interface {
	Search(context.Context, []float32, int, SearchOptions) (SearchResult, error)
	Len() int
	Dimensions() int
	Metric() Metric
}
