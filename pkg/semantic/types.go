// Package semantic provides chunk-aware dense-vector search.
package semantic

import (
	"errors"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

var (
	ErrInvalidConfig     = errors.New("semantic: invalid configuration")
	ErrInvalidBatch      = errors.New("semantic: invalid document batch")
	ErrDocumentExists    = errors.New("semantic: document already exists")
	ErrDocumentNotFound  = errors.New("semantic: document not found")
	ErrDuplicateChunkID  = errors.New("semantic: duplicate chunk ID")
	ErrVectorIDExhausted = errors.New("semantic: vector ID exhausted")
	ErrCapacityExceeded  = errors.New("semantic: vector capacity exceeded")
	ErrInternalState     = errors.New("semantic: inconsistent internal state")
	ErrInvalidSnapshot   = errors.New("semantic: invalid snapshot")
)

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
	Space                   SpaceDescriptor
	Chunking                ChunkingDescriptor
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

type ChunkHit struct {
	Ref      chunk.Ref
	Distance float64
}

type ChunkSearchResult struct {
	Hits       []ChunkHit
	Stats      vector.SearchStats
	Incomplete bool
}

// SearchOptions controls request-local ANN work. Zero values use the HNSW
// defaults configured for the service or segment.
type SearchOptions struct {
	EfSearch   int
	VisitLimit int
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

// VectorRow describes the semantic identity of one segment row. Its index in
// Segment.Rows is the row's local ordinal for that segment.
// Rows retain monotonically allocated VectorID order across compaction.
type VectorRow struct {
	VectorID VectorID
	Chunk    chunk.Ref
}
