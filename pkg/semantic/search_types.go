package semantic

import (
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

// ChunkHit identifies one matching source chunk and its vector distance from
// the query. Smaller distances are better.
type ChunkHit struct {
	Ref      Ref
	Distance float64
}

// DocumentHit contains the best matching chunks for one document. Distance is
// the smallest chunk distance. Chunks are ordered by ascending distance and
// limited by the configured service limit.
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
	// HNSWSearches is the number of segment-level HNSW searches actually run.
	HNSWSearches int
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
