package vector

import (
	"context"
)

// PreparedVectorStore provides immutable prepared vector rows by ordinal.
// Implementations must support concurrent reads, fill the destination
// completely, and not retain it.
type PreparedVectorStore interface {
	Len() int
	Dimensions() int
	Metric() Metric
	Normalization() Normalization
	ReadVectorInto(context.Context, Ordinal, []float32) error
}
