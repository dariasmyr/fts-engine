package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
)

type ChunkVector struct {
	Ref    chunk.Ref
	Vector []float32
}

// Document is the application-level input for semantic ingestion and query.
// Its fields are encoded into chunks and embeddings by an Encoder.
type Document struct {
	ID     fts.DocID
	Fields map[string]string
}

// Encoder converts a document into prepared chunk vectors. The semantic
// service owns the index operation; an encoder only owns this transformation.
// Implementations passed to concurrent Service operations must be safe for
// concurrent use.
type Encoder interface {
	Encode(context.Context, Document) ([]ChunkVector, error)
	Descriptor() PipelineDescriptor
}
