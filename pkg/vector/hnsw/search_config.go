package hnsw

// SearchConfig bounds request-local HNSW search work and allocations.
type SearchConfig struct {
	// DefaultEfSearch is used when SearchOptions.EfSearch is zero.
	DefaultEfSearch int
	// MaxEfSearch bounds the accepted request value and result heap.
	MaxEfSearch int
	// DefaultVisitLimit is used when SearchOptions.VisitLimit is zero.
	DefaultVisitLimit int
	// MaxVisitLimit bounds uniquely scored graph nodes per request.
	MaxVisitLimit int
	// MaxK bounds the number of returned hits.
	MaxK int
}

func (c SearchConfig) validate() error {
	if c.DefaultEfSearch <= 0 || c.MaxEfSearch < c.DefaultEfSearch {
		return ErrInvalidSearchConfig
	}
	if c.DefaultVisitLimit <= 0 || c.MaxVisitLimit < c.DefaultVisitLimit {
		return ErrInvalidSearchConfig
	}
	if c.MaxK <= 0 || c.MaxK > c.MaxEfSearch {
		return ErrInvalidSearchConfig
	}
	return nil
}
