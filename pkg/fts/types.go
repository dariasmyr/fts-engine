package fts

import (
	"context"
	"io"
)

type DocID string

type DocRef struct {
	ID    DocID
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
	Timings           map[string]string
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
	Insert(key string, id DocID) error
	Search(key string) ([]DocRef, error)
}

type PrefixIndex interface {
	Index
	SearchPrefix(prefix string) ([]DocRef, error)
}

type PositionalDocRef struct {
	ID DocID
	// Positions may share backing storage with the index and must be treated as read-only.
	Positions []uint32
}

type PositionalIndex interface {
	Index
	InsertAt(key string, id DocID, position uint32) error
	SearchPositional(key string) ([]PositionalDocRef, error)
}

type IndexFactory func(fieldName string) (Index, error)

type Analyzer interface {
	Analyze() Stats
}

type Serializable interface {
	Serialize(w io.Writer) error
}

type IndexLoader func(r io.Reader) (Index, error)

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

type Engine interface {
	IndexDocument(ctx context.Context, docID DocID, content string) error
	Search(ctx context.Context, q Query, maxResults int) (*SearchResult, error)
	SearchFieldClauses(ctx context.Context, clauses []FieldQueryClause, maxResults int) (*SearchResult, error)
	SearchDocuments(ctx context.Context, query string, maxResults int) (*SearchResult, error)
}

func WordKeys(token string) ([]string, error) {
	return []string{token}, nil
}
