package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

// EncodedChunk couples source-chunk metadata with its prepared embedding.
type EncodedChunk struct {
	Ref    Ref
	Vector []float32
}

// Encoder converts a document into prepared chunk vectors. The semantic
// service owns the index operation; an encoder only owns this transformation.
// Implementations passed to concurrent Service operations must be safe for
// concurrent use.
type Encoder interface {
	Encode(context.Context, fts.Document) ([]EncodedChunk, error)
	Descriptor() Schema
}
