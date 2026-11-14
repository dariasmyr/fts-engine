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

// Validate checks request-local HNSW tuning independently of index size and
// result-filter cardinality. Zero values select the index defaults.
func (c QueryConfig) Validate() error {
	if c.EfSearch < 0 || c.VisitLimit < 0 {
		return ErrInvalidQueryConfig
	}
	return nil
}

// SearchStats describes HNSW traversal work for one request.
type SearchStats struct {
	// ConfiguredEfSearch is the query override, or the index default when the
	// override is zero.
	ConfiguredEfSearch int
	// AppliedEfSearch is the runtime limit after raising ConfiguredEfSearch to
	// at least k and capping it by the index size. It is zero for an empty index.
	AppliedEfSearch      int
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
