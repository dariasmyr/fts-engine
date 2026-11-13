package semantic

import "github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"

// QueryConfig controls request-local semantic search budgets.
type QueryConfig struct {
	EfSearch        int
	VisitLimit      int
	CandidateChunks int
}

// Validate checks request-local query values independently of index ceilings.
func (c QueryConfig) Validate() error {
	if c.CandidateChunks < 0 {
		return ErrInvalidQueryConfig
	}
	if err := (hnsw.QueryConfig{EfSearch: c.EfSearch, VisitLimit: c.VisitLimit}).Validate(); err != nil {
		return ErrInvalidQueryConfig
	}
	return nil
}
