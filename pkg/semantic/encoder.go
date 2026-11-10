package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
)

// ChunkEmbedding couples source-chunk metadata with one raw finite embedding.
// Index validates, copies, and performs metric-specific preparation.
type ChunkEmbedding struct {
	Ref       Ref
	Embedding []float32
}

// DocumentEncoder converts source documents into raw chunk embeddings.
// Implementations passed to concurrent Service operations must be safe for
// concurrent use.
type DocumentEncoder interface {
	EncodeDocument(context.Context, fts.Document) ([]ChunkEmbedding, error)
}

// QueryEncoder converts direct text and advanced document queries into raw
// embeddings. Query preparation may differ from document chunking.
type QueryEncoder interface {
	EncodeQuery(context.Context, string, QueryConfig) ([]ChunkEmbedding, error)
	EncodeQueryDocument(context.Context, fts.Document, QueryConfig) ([]ChunkEmbedding, error)
}
