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
	ErrInvalidSnapshot      = errors.New("semantic: invalid snapshot")
	ErrInvalidSearchOptions = errors.New("semantic: invalid search options")
)

// VectorID is the stable internal identity of one stored embedding vector.
// It is allocated monotonically and is not reused after replacement or
// deletion. It is separate from chunk.ID: the former identifies an index row,
// while the latter identifies the source chunk returned to the caller.
type VectorID uint64

type ComponentID uint64

const MutableHeadID ComponentID = 1

type SpaceDescriptor struct {
	ID                  string
	ModelVersion        string
	Fingerprint         string
	Dimensions          int
	Metric              vector.Metric
	Normalization       vector.Normalization
	VectorFormatVersion uint32
}

type ChunkingDescriptor struct {
	ID          string
	Version     uint32
	Fingerprint string
}

type Config struct {
	Space    SpaceDescriptor
	Chunking ChunkingDescriptor
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
