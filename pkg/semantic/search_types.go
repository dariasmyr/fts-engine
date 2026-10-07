package semantic

import (
	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

// ChunkHit identifies one matching source chunk and its vector distance from
// the query. Smaller distances are better.
type ChunkHit struct {
	Ref      chunk.Ref
	Distance float64
}

// SearchOptions controls ANN work and document grouping across the complete
// semantic request, including all encoded query chunks and index segments.
type SearchOptions struct {
	// EfSearch controls candidate breadth for each underlying HNSW search. Zero
	// uses the configured HNSW default.
	EfSearch int
	// VisitLimit bounds the total visited nodes across all encoded query chunks
	// and segments. Zero uses the configured HNSW default as the request budget.
	VisitLimit int
	// CandidateChunks bounds the total chunk candidates collected before
	// document grouping. Zero uses Config.Limits.MaxChunkCandidates.
	CandidateChunks int
}

// DocumentHit contains the best matching chunks for one document. Distance is
// the smallest chunk distance. Chunks are ordered by ascending distance and
// limited by Config.Limits.MaxChunksPerDocumentHit.
type DocumentHit struct {
	DocID    fts.DocID
	Distance float64
	Chunks   []ChunkHit
}

// DocumentSearchResult contains document hits ordered by ascending distance
// and then document ID. For queries encoded into multiple chunks, diagnostics
// are aggregated across all encoded query chunks.
type DocumentSearchResult struct {
	Hits []DocumentHit
	// CandidateChunks is the total number of chunk candidates considered.
	CandidateChunks int
	// DistinctDocuments is counted before Hits is limited to the requested
	// maximum result count.
	DistinctDocuments int
	// GroupingIncomplete reports that the candidate budget did not cover all
	// live chunks or that an underlying ANN search terminated early.
	GroupingIncomplete bool
	// Stats contains the aggregated vector-search work.
	Stats hnsw.SearchStats
}
