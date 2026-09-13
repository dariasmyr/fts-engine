package hnsw

import (
	"context"
	"errors"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/internal/exactsearch"
)

var ErrInvalidExactFallbackPolicy = errors.New("vector/hnsw: invalid exact fallback policy")

// ExactFallbackPolicy enables exact search when every configured inclusive
// threshold permits it. A zero threshold is ignored; no defaults apply.
type ExactFallbackPolicy struct {
	MaxPhysicalRows         int
	MaxDistanceComputations int
}

func (p ExactFallbackPolicy) validate() error {
	if p.MaxPhysicalRows < 0 || p.MaxDistanceComputations < 0 {
		return ErrInvalidExactFallbackPolicy
	}
	return nil
}

// ExactFallbackSearcher chooses between exact matrix scan and HNSW per request.
// Its underlying Reader remains immutable and Reader.Search remains forced ANN.
type ExactFallbackSearcher struct {
	reader *Reader
	policy ExactFallbackPolicy
}

func NewExactFallbackSearcher(reader *Reader, policy ExactFallbackPolicy) (*ExactFallbackSearcher, error) {
	if reader == nil || !reader.validated {
		return nil, ErrInvalidGraph
	}
	if err := policy.validate(); err != nil {
		return nil, err
	}
	return &ExactFallbackSearcher{reader: reader, policy: policy}, nil
}

// WithExactFallback wraps r with an explicit per-segment fallback policy.
func (r *Reader) WithExactFallback(policy ExactFallbackPolicy) (*ExactFallbackSearcher, error) {
	return NewExactFallbackSearcher(r, policy)
}

func (s *ExactFallbackSearcher) Search(ctx context.Context, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	if s == nil || s.reader == nil || !s.reader.validated {
		return vector.SearchResult{}, ErrInvalidGraph
	}
	if ctx == nil {
		return vector.SearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return vector.SearchResult{}, err
	}
	if err := s.policy.validate(); err != nil {
		return vector.SearchResult{}, err
	}
	config := s.reader.searchConfig
	if err := config.validate(); err != nil {
		return vector.SearchResult{}, err
	}
	if k <= 0 || k > config.MaxK {
		return vector.SearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, config.MaxK)
	}
	if options.EfSearch < 0 || options.VisitLimit < 0 {
		return vector.SearchResult{}, vector.ErrInvalidSearchOptions
	}
	efSearch := options.EfSearch
	if efSearch == 0 {
		efSearch = config.DefaultEfSearch
	}
	if max(k, efSearch) > config.MaxEfSearch {
		return vector.SearchResult{}, fmt.Errorf("%w: efSearch=%d max=%d", vector.ErrInvalidSearchOptions, max(k, efSearch), config.MaxEfSearch)
	}
	visitLimit := options.VisitLimit
	if visitLimit == 0 {
		visitLimit = config.DefaultVisitLimit
	}
	if visitLimit > config.MaxVisitLimit {
		return vector.SearchResult{}, fmt.Errorf("%w: visitLimit=%d max=%d", vector.ErrInvalidSearchOptions, visitLimit, config.MaxVisitLimit)
	}
	allowedCount, err := exactsearch.AllowedCount(s.reader.Len(), options.ResultFilter)
	if err != nil {
		return vector.SearchResult{}, err
	}

	useExact := s.policy.MaxPhysicalRows > 0 || s.policy.MaxDistanceComputations > 0
	if s.policy.MaxPhysicalRows > 0 && s.reader.Len() > s.policy.MaxPhysicalRows {
		useExact = false
	}
	if s.policy.MaxDistanceComputations > 0 && allowedCount > s.policy.MaxDistanceComputations {
		useExact = false
	}
	// Exact scan must inspect every physical row. Preserve the request's work
	// budget by keeping ANN when the resolved visit limit cannot complete it.
	if visitLimit < s.reader.Len() {
		useExact = false
	}
	if !useExact {
		return s.reader.Search(ctx, query, k, options)
	}
	exactOptions := vector.SearchOptions{VisitLimit: visitLimit, ResultFilter: options.ResultFilter}
	result, err := exactsearch.SearchSource(ctx, s.reader.source, config.MaxK, query, k, exactOptions)
	if err != nil {
		return vector.SearchResult{}, err
	}
	result.Stats.UsedExactFallback = true
	return result, nil
}

func (s *ExactFallbackSearcher) Len() int {
	if s == nil || s.reader == nil {
		return 0
	}
	return s.reader.Len()
}

func (s *ExactFallbackSearcher) Dimensions() int {
	if s == nil || s.reader == nil {
		return 0
	}
	return s.reader.Dimensions()
}

func (s *ExactFallbackSearcher) Metric() vector.Metric {
	if s == nil || s.reader == nil {
		return 0
	}
	return s.reader.Metric()
}

func (s *ExactFallbackSearcher) Policy() ExactFallbackPolicy {
	if s == nil {
		return ExactFallbackPolicy{}
	}
	return s.policy
}
