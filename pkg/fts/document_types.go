package fts

type DocID string

type DocOrd uint32

type Posting struct {
	Ord   DocOrd
	Count uint32
	Seq   uint32
}

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

const DefaultField = "_default"

type Document struct {
	ID     DocID
	Fields map[string]Field
}

type Field struct {
	Value    string
	Pipeline Pipeline
}

type PositionalPosting struct {
	Ord DocOrd
	// Positions may share backing storage with the index and must be treated as read-only.
	Positions []uint32
}
