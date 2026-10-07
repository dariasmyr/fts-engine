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
	chunker     Chunker
	embedder    EmbeddingProvider
	descriptors semantic.Schema
}

func New(chunker Chunker, embedder EmbeddingProvider, embedding semantic.EmbeddingDescriptor, chunking semantic.ChunkingDescriptor) (*DocumentEncoder, error) {
	if embedder == nil {
		return nil, ErrInvalidConfig
	}
	return &DocumentEncoder{chunker: chunker, embedder: embedder, descriptors: semantic.Schema{Embedding: embedding, Chunking: chunking}}, nil
}

// Descriptor returns the immutable model and chunking identity used by Encode.
func (e *DocumentEncoder) Descriptor() semantic.Schema { return e.descriptors }

// Encode chunks and embeds a complete document and returns prepared vectors
// for the semantic service's internal encoded stage.
func (e *DocumentEncoder) Encode(ctx context.Context, document fts.Document) ([]semantic.EncodedChunk, error) {
	chunks, err := e.chunkDocument(ctx, document)
	if err != nil {
		return nil, err
	}
	vectors, err := e.embedder.Embed(ctx, chunks)
	if err != nil {
		return nil, err
	}
	return makeChunkVectors(chunks, vectors)
}

func (e *DocumentEncoder) chunkDocument(ctx context.Context, document fts.Document) ([]chunk.Chunk, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if document.ID == "" || len(document.Fields) == 0 {
		return nil, ErrInvalidDocument
	}
	chunks := make([]chunk.Chunk, 0, len(document.Fields))
	fields := make([]string, 0, len(document.Fields))
	for field := range document.Fields {
		fields = append(fields, field)
	}
	slices.Sort(fields)
	for _, field := range fields {
		value := document.Fields[field]
		if field == "" || value.Text == "" {
			return nil, ErrInvalidDocument
		}

		if e.chunker == nil {
			whole, err := chunk.Whole(document.ID, field, value.Text)
			if err != nil {
				return nil, err
			}

			if whole.Ref.ID != "" {
				chunks = append(chunks, whole)
			}

			continue
		}

		fieldChunks, err := e.chunker.Split(document.ID, field, value.Text)
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

func makeChunkVectors(chunks []chunk.Chunk, vectors [][]float32) ([]semantic.EncodedChunk, error) {
	if len(chunks) != len(vectors) {
		return nil, fmt.Errorf("%w: got %d vectors for %d chunks", ErrEmbeddingCountMismatch, len(vectors), len(chunks))
	}
	result := make([]semantic.EncodedChunk, len(chunks))
	for i, item := range chunks {
		result[i] = semantic.EncodedChunk{Ref: item.Ref, Vector: vectors[i]}
	}
	return result, nil
}
