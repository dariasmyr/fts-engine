// Package semantic provides chunk-aware in-memory dense-vector search.
package semantic

import (
	"errors"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

var (
	ErrInvalidConfig     = errors.New("semantic: invalid configuration")
	ErrInvalidBatch      = errors.New("semantic: invalid document batch")
	ErrDocumentExists    = errors.New("semantic: document already exists")
	ErrDocumentNotFound  = errors.New("semantic: document not found")
	ErrDuplicateChunkID  = errors.New("semantic: duplicate chunk ID")
	ErrVectorIDExhausted = errors.New("semantic: vector ID exhausted")
	ErrInternalState     = errors.New("semantic: inconsistent internal state")
	ErrInvalidCheckpoint = errors.New("semantic: invalid checkpoint")
)

type VectorID uint64

type ComponentID uint64

const MutableHeadID ComponentID = 1

type Location struct {
	Component ComponentID
	Ordinal   vector.Ordinal
}

type SpaceDescriptor struct {
	ID                  string
	Dimensions          int
	Metric              vector.Metric
	Normalization       vector.Normalization
	VectorFormatVersion uint32
}

type ChunkingDescriptor struct {
	ID string
}

type Config struct {
	Space                        SpaceDescriptor
	Chunking                     ChunkingDescriptor
	MaxVectors                   int
	MaxChunksPerDocument         int
	MaxK                         int
	MaxChunkCandidates           int
	MaxChunksPerDocumentHit      int
	InitialVectorCapacity        int
	InitialVectorIDHighWatermark VectorID
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
	HighWatermark   VectorID
}

type DocumentRecord struct {
	DocID     fts.DocID
	VectorIDs []VectorID
}

type RefRecord struct {
	VectorID VectorID
	Ref      chunk.Ref
}

type DuplicateStatistics struct {
	VectorRows      int
	UniqueVectors   int
	DuplicateRows   int
	DuplicateGroups int
	MaxFanOut       int
}
