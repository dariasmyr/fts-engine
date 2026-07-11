package fts

import (
	"io"
)

type DocID string

type DocOrd uint32

type Posting struct {
	Ord   DocOrd
	Count uint32
	Seq   uint32
}

type DocRef = Posting

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

type Index interface {
	Insert(key string, ord DocOrd) error
	Search(key string) ([]Posting, error)
}

type PrefixIndex interface {
	Index
	SearchPrefix(prefix string) ([]Posting, error)
}

type PositionalPosting struct {
	Ord DocOrd
	// Positions may share backing storage with the index and must be treated as read-only.
	Positions []uint32
}

type PositionalDocRef = PositionalPosting

type PositionalIndex interface {
	Index
	InsertAt(key string, position uint32, ord DocOrd) error
	SearchPositional(key string) ([]PositionalPosting, error)
}

type IndexFactory func(fieldName string) (Index, error)

type Analyzer interface {
	Analyze() Stats
}

type Serializable interface {
	Serialize(w io.Writer) error
}

type KeyGenerator func(token string) ([]string, error)

type Pipeline interface {
	Process(text string) []string
}

// Filter in a dynamic (bloom, cuckoo filters) that allow write on read
type Filter interface {
	Add(item []byte) bool
	Contains(item []byte) bool
}

type BuildableFilter interface {
	Build() error
}

// StaticFilter describes filter built from replayable key stream.
type StaticFilter interface {
	BuildFromKeyStream(stream func(func([]byte) bool) error) error
	Contains(item []byte) bool
}

type RetryableStaticFilter interface {
	StaticFilter
	BuildWithRetriesFromKeyStream(stream func(func([]byte) bool) error, maxAttempts uint32) error
}

func WordKeys(token string) ([]string, error) {
	return []string{token}, nil
}
