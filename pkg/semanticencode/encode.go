// Package semanticencode converts source documents into prepared semantic
// vectors. It does not own or mutate a semantic index.
package semanticencode

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

var (
	ErrInvalidConfig          = errors.New("semanticencode: invalid configuration")
	ErrInvalidDocument        = errors.New("semanticencode: invalid document")
	ErrEmbeddingCountMismatch = errors.New("semanticencode: embedding count does not match chunks")
)

// Document is the complete source document submitted for semantic encoding.
// Fields are processed in lexicographic field-name order for deterministic
// chunk and embedding ordering.
type Document = semantic.Document

// Chunker splits one document field into deterministic source chunks. A nil
// Chunker makes the encoder create one whole-field chunk.
type Chunker interface {
	Split(fts.DocID, string, string) ([]chunk.Chunk, error)
}

// EmbeddingProvider converts chunks into embeddings in the same order as the
// input slice. The provider owns model-specific preprocessing and batching.
type EmbeddingProvider interface {
	Embed(context.Context, []chunk.Chunk) ([][]float32, error)
}

// DocumentEncoder owns document-level chunking and embedding orchestration.
// The resulting vectors can be passed to semantic.Service or another sink.
type DocumentEncoder struct {
	chunker  Chunker
	embedder EmbeddingProvider
}

func New(chunker Chunker, embedder EmbeddingProvider) (*DocumentEncoder, error) {
	if embedder == nil {
		return nil, ErrInvalidConfig
	}
	return &DocumentEncoder{chunker: chunker, embedder: embedder}, nil
}

// Encode chunks and embeds a complete document and returns prepared vectors
// for the semantic service's internal encoded stage.
func (e *DocumentEncoder) Encode(ctx context.Context, document Document) ([]semantic.ChunkVector, error) {
	chunks, err := e.prepare(ctx, document)
	if err != nil {
		return nil, err
	}
	vectors, err := e.embedder.Embed(ctx, chunks)
	if err != nil {
		return nil, err
	}
	return makeChunkVectors(chunks, vectors)
}

func (e *DocumentEncoder) prepare(ctx context.Context, document Document) ([]chunk.Chunk, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if document.ID == "" || len(document.Fields) == 0 {
		return nil, ErrInvalidDocument
	}
	fields := make([]string, 0, len(document.Fields))
	for field := range document.Fields {
		if field == "" {
			return nil, ErrInvalidDocument
		}
		fields = append(fields, field)
	}
	slices.Sort(fields)
	chunks := make([]chunk.Chunk, 0, len(fields))
	for _, field := range fields {
		text := document.Fields[field]
		var fieldChunks []chunk.Chunk
		var err error
		if e.chunker == nil {
			var whole chunk.Chunk
			whole, err = chunk.Whole(document.ID, field, text)
			if err == nil && whole.Ref.ID != "" {
				fieldChunks = []chunk.Chunk{whole}
			}
		} else {
			fieldChunks, err = e.chunker.Split(document.ID, field, text)
		}
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, fieldChunks...)
	}
	if len(chunks) == 0 {
		return nil, ErrInvalidDocument
	}
	return chunks, nil
}

func makeChunkVectors(chunks []chunk.Chunk, vectors [][]float32) ([]semantic.ChunkVector, error) {
	if len(chunks) != len(vectors) {
		return nil, fmt.Errorf("%w: got %d vectors for %d chunks", ErrEmbeddingCountMismatch, len(vectors), len(chunks))
	}
	result := make([]semantic.ChunkVector, len(chunks))
	for i, item := range chunks {
		result[i] = semantic.ChunkVector{Ref: item.Ref, Vector: vectors[i]}
	}
	return result, nil
}
