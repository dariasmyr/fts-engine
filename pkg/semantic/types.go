// Package semantic provides chunk-aware dense-vector search.
package semantic

import (
	"context"
	"errors"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

var (
	ErrInvalidConfig        = errors.New("semantic: invalid configuration")
	ErrInvalidBatch         = errors.New("semantic: invalid document batch")
	ErrDocumentExists       = errors.New("semantic: document already exists")
	ErrDocumentNotFound     = errors.New("semantic: document not found")
	ErrVectorIDExhausted    = errors.New("semantic: vector ID exhausted")
	ErrCapacityExceeded     = errors.New("semantic: vector capacity exceeded")
	ErrInternalState        = errors.New("semantic: inconsistent internal state")
	ErrInvalidSegment       = errors.New("semantic: invalid segment")
	ErrEmbeddingMismatch    = errors.New("semantic: embedding descriptor mismatch")
	ErrChunkingMismatch     = errors.New("semantic: chunking descriptor mismatch")
	ErrInvalidSearchOptions = errors.New("semantic: invalid search options")
)

// VectorID is the stable internal identity of one stored embedding vector.
// It is allocated monotonically and is not reused after replacement or
// deletion. It is separate from chunk.ID: the former identifies an index row,
// while the latter identifies the source chunk returned to the caller.
type VectorID uint64

type ComponentID uint64

const MutableHeadID ComponentID = 1

type VectorSpec struct {
	Dimensions          int
	Metric              vector.Metric
	VectorFormatVersion uint32
}

func (spec VectorSpec) VectorSpace() (vector.Space, error) {
	return vector.NewSpace(spec.Dimensions, spec.Metric)
}

type EmbeddingDescriptor struct {
	ProviderID          string
	ModelID             string
	ModelVersion        string
	PipelineFingerprint string
	Vector              VectorSpec
}

func NewEmbeddingDescriptor(providerID, modelID, modelVersion, pipelineFingerprint string, vectorSpec VectorSpec) (EmbeddingDescriptor, error) {
	if _, err := vectorSpec.VectorSpace(); err != nil {
		return EmbeddingDescriptor{}, err
	}
	if vectorSpec.VectorFormatVersion == 0 {
		return EmbeddingDescriptor{}, ErrInvalidConfig
	}
	return EmbeddingDescriptor{
		ProviderID: providerID, ModelID: modelID, ModelVersion: modelVersion,
		PipelineFingerprint: pipelineFingerprint, Vector: vectorSpec,
	}, nil
}

func (descriptor EmbeddingDescriptor) VectorSpace() (vector.Space, error) {
	vectorSpace, err := descriptor.Vector.VectorSpace()
	if err != nil {
		return vector.Space{}, err
	}
	return vectorSpace, nil
}

type ChunkingDescriptor struct {
	ID          string
	Version     uint32
	Fingerprint string
}

type PipelineDescriptor struct {
	Embedding EmbeddingDescriptor
	Chunking  ChunkingDescriptor
}

func (descriptor EmbeddingDescriptor) IsValid() bool {
	return descriptor.ProviderID != "" && descriptor.ModelID != "" && descriptor.ModelVersion != "" &&
		descriptor.PipelineFingerprint != "" && descriptor.Vector.Dimensions > 0 && descriptor.Vector.VectorFormatVersion > 0 && descriptor.Vector.Metric.Valid()
}

func (descriptor ChunkingDescriptor) IsValid() bool {
	return descriptor.ID != "" && descriptor.Version > 0 && descriptor.Fingerprint != ""
}

func descriptorsEqual(left, right PipelineDescriptor) (bool, error) {
	if left.Embedding != right.Embedding {
		return false, ErrEmbeddingMismatch
	}
	if left.Chunking != right.Chunking {
		return false, ErrChunkingMismatch
	}
	return true, nil
}

type Config struct {
	Embedding EmbeddingDescriptor
	Chunking  ChunkingDescriptor
	// MaxVectors limits the number of live vectors accepted by the service.
	// Replaced and deleted physical rows remain until Compact.
	MaxVectors              int
	MaxChunksPerDocument    int
	MaxK                    int
	MaxChunkCandidates      int
	MaxChunksPerDocumentHit int
	InitialVectorCapacity   int
	HNSWBuild               hnsw.BuildConfig
	HNSWSearch              hnsw.SearchConfig
	// InitialMaxAllocatedVectorID seeds the allocator; the first new vector gets
	// the following ID. Use it when continuing an existing ID namespace.
	InitialMaxAllocatedVectorID VectorID
}

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

type ChunkHit struct {
	Ref      chunk.Ref
	Distance float64
}

type chunkSearchResult struct {
	Hits       []ChunkHit
	Stats      vector.SearchStats
	Incomplete bool
}

// SearchOptions controls request-local ANN work. Zero values use the HNSW
// defaults configured for the service or segment.
type SearchOptions struct {
	EfSearch   int
	VisitLimit int
	// CandidateChunks controls how many chunk candidates are collected before
	// document grouping. Zero uses Config.MaxChunkCandidates.
	CandidateChunks int
}

type DocumentHit struct {
	DocID    fts.DocID
	Distance float64
	Chunks   []ChunkHit
}

type DocumentSearchResult struct {
	Hits               []DocumentHit
	CandidateChunks    int
	DistinctDocuments  int
	GroupingIncomplete bool
	Stats              vector.SearchStats
}

type Statistics struct {
	Documents       int
	PhysicalVectors int
	LiveVectors     int
	StaleVectors    int
	// MaxAllocatedVectorID never decreases after replacement, deletion, or compaction.
	MaxAllocatedVectorID VectorID
}

// VectorRow connects an internal vector identity with the source chunk it
// represents. Segment.Rows stores these rows by local ordinal, so the ordinal
// may change when segments are compacted while VectorID and Chunk remain the
// same. For example, VectorID 42 can move from ordinal 5 to ordinal 0 without
// changing which document chunk it represents.
type VectorRow struct {
	VectorID VectorID
	Chunk    chunk.Ref
}
