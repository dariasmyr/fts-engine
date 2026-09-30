package vectorstore

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// PreparedVectorStore provides immutable prepared vector rows by ordinal.
// Implementations must support concurrent reads, fill the destination
// completely, and not retain it.
type PreparedVectorStore interface {
	Len() int
	Dimensions() int
	Metric() vector.Metric
	Normalization() vector.Normalization
	ReadVectorInto(context.Context, vector.Ordinal, []float32) error
}
