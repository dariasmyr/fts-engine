package fts

import "github.com/dariasmyr/fts-engine/v2/pkg/posting"

type DocID string

type DocOrd = posting.Ord

type Posting = posting.Posting

type Result struct {
	ID            DocID
	UniqueMatches int
	TotalMatches  int
	Score         float64
}

type SearchResult struct {
	Results           []Result
	TotalResultsCount int
	Diagnostics       *QueryDiagnostics
}

// SearchOptions configures typed and text searches. Nil CandidateIDs means
// unrestricted search; a non-nil empty slice matches no documents. Unknown,
// duplicate, and deleted IDs are ignored. DefaultField applies to unqualified
// clauses parsed by SearchText. A non-nil Fields slice restricts typed and text
// queries to the listed fields; an empty slice matches no fields.
type SearchOptions struct {
	MaxResults   int
	CandidateIDs []DocID
	DefaultField string
	Fields       []string
}

const DefaultField = "_default"

type Document struct {
	ID     DocID
	Fields map[string]Field
}

type Field struct {
	Value string
}

type PositionalPosting = posting.PositionalPosting
