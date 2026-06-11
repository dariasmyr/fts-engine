package harness

import (
	"context"
	"io"
)

type Document struct {
	ID   string
	Body string
}

type Query struct {
	ID   string
	Text string
	K    int
}

type SearchHit struct {
	DocID string
	Score float64
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
