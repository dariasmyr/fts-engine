package hnsw

import "github.com/dariasmyr/fts-engine/v2/pkg/vector"

const (
	TerminationComplete   = "complete"
	TerminationVisitLimit = "visit_limit"
)

// QueryConfig contains request-local HNSW tuning.
type QueryConfig struct {
	EfSearch     int
	VisitLimit   int
	ResultFilter vector.ResultFilter
}

// SearchStats describes HNSW traversal work for one request.
type SearchStats struct {
	VisitedNodes         int
	ExpandedNodes        int
	UpperExpandedNodes   int
	Level0ExpandedNodes  int
	DistanceComputations int
	RejectedNodes        int
	Termination          string
}

// SearchResult contains ordered vector hits and HNSW execution details.
type SearchResult struct {
	Hits       []vector.Hit
	Stats      SearchStats
	Incomplete bool
}
