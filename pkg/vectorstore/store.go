package vectorstore

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// PreparedVectorStore provides immutable prepared vector rows by ordinal.
// Implementations must fill the destination completely and must not retain it.
type PreparedVectorStore interface {
	Len() int
	Dimensions() int
	Metric() vector.Metric
	Normalization() vector.Normalization
	ReadVectorInto(context.Context, vector.Ordinal, []float32) error
}
