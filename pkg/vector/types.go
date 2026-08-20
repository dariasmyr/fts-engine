// Package vector defines shared contracts for dense-vector search indexes.
package vector

import "context"

// Metric defines how vectors are ordered. Every supported metric returns a
// distance where a smaller value is better.
type Metric uint8

const (
	MetricCosine Metric = iota + 1
	MetricL2Squared
)

func (m Metric) String() string {
	switch m {
	case MetricCosine:
		return "cosine"
	case MetricL2Squared:
		return "l2_squared"
	default:
		return "unknown"
	}
}

// Valid reports whether the metric is supported.
func (m Metric) Valid() bool {
	return m == MetricCosine || m == MetricL2Squared
}

// Normalization describes how vectors are stored before distance evaluation.
type Normalization uint8

const (
	NormalizationNone Normalization = iota
	NormalizationUnitLength
)

func (n Normalization) String() string {
	switch n {
	case NormalizationNone:
		return "none"
	case NormalizationUnitLength:
		return "unit_length"
	default:
		return "unknown"
	}
}

// Ordinal is a dense index-local vector row number. It is not a durable
// document, chunk, or vector identity.
type Ordinal uint32

// Hit is one vector-search result. Hits are ordered by ascending Distance and
// then ascending Ordinal.
type Hit struct {
	Ordinal  Ordinal
	Distance float64
}

// AcceptSet is an immutable snapshot that controls result eligibility. Rejected
// HNSW nodes may still be used for graph traversal. A nil AcceptSet means that
// every ordinal is accepted. Searchers must reject sets whose Size differs from
// their vector-row count.
type AcceptSet interface {
	Contains(Ordinal) bool
	Cardinality() int
	Size() uint32
}

type SearchOptions struct {
	EfSearch   int
	VisitLimit int
	Accept     AcceptSet
}

type SearchStats struct {
	VisitedNodes         int
	ExpandedNodes        int
	DistanceComputations int
	RejectedNodes        int
	UsedExactFallback    bool
	Termination          string
}

type SearchResult struct {
	Hits       []Hit
	Stats      SearchStats
	Incomplete bool
}

// Searcher is implemented by mutable and immutable vector indexes.
type Searcher interface {
	Search(context.Context, []float32, int, SearchOptions) (SearchResult, error)
	Len() int
	Dimensions() int
	Metric() Metric
}
