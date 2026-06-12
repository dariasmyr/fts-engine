package harness

import (
	"context"
	"io"
)

type Document struct {
	ID   string
	Body string
}

type QueryKind string

const (
	QueryKindText    QueryKind = "text"
	QueryKindTerm    QueryKind = "term"
	QueryKindPhrase  QueryKind = "phrase"
	QueryKindPrefix  QueryKind = "prefix"
	QueryKindBoolean QueryKind = "boolean"
)

type BoolOccur string

const (
	OccurMust    BoolOccur = "must"
	OccurShould  BoolOccur = "should"
	OccurMustNot BoolOccur = "must_not"
)

type Atom struct {
	Kind QueryKind
	Text string
}

type BoolClause struct {
	Occur BoolOccur
	Atom  Atom
}

type BoolQuery struct {
	Clauses []BoolClause
}

type Query struct {
	ID      string
	Kind    QueryKind
	Text    string
	K       int
	Class   string
	Boolean *BoolQuery
}

type SearchHit struct {
	DocID string
	Score float64
}

type MetadataProvider interface {
	BenchmarkMetadata() map[string]any
}

type ExtrasProvider interface {
	BenchmarkExtras() map[string]any
}

// Engine lifecycle: Open -> (Index... Commit)* -> (Search)* -> Close.
type Engine interface {
	Name() string

	Open(ctx context.Context, dir string) error
	Index(ctx context.Context, docs []Document) error
	Commit(ctx context.Context) error
	Search(ctx context.Context, q Query) ([]SearchHit, error)
	IndexSizeBytes() (int64, error)

	io.Closer
}
