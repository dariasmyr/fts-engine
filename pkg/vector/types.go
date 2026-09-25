// Package vector defines shared contracts for dense-vector search indexes.
package vector

import (
	"context"
	"errors"
)

var (
	ErrNilContext               = errors.New("vector: nil context")
	ErrInvalidK                 = errors.New("vector: k must be positive")
	ErrInvalidSearchOptions     = errors.New("vector: search options must not be negative")
	ErrResultFilterSizeMismatch = errors.New("vector: result filter size does not match vector count")
)

const (
	TerminationComplete   = "complete"
	TerminationVisitLimit = "visit_limit"
)

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

// ResultFilter is an immutable snapshot controlling which local ordinals may
// appear in search hits. TotalOrdinalCount must match the searcher's Len.
type ResultFilter interface {
	Allows(Ordinal) bool
	AllowedOrdinalCount() int
	TotalOrdinalCount() uint32
}

type SearchOptions struct {
	EfSearch     int
	VisitLimit   int
	ResultFilter ResultFilter
}

type SearchStats struct {
	VisitedNodes         int
	ExpandedNodes        int
	DistanceComputations int
	RejectedNodes        int
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

// PreparedVectorSource provides immutable prepared vector rows by ordinal.
// Implementations must fill the destination completely and must not retain it.
type PreparedVectorSource interface {
	Len() int
	Dimensions() int
	Metric() Metric
	Normalization() Normalization
	ReadVectorInto(context.Context, Ordinal, []float32) error
}
