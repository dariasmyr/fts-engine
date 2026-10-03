package semantic

import (
	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

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
