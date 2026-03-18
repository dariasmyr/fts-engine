package fts

import (
	"context"
	"io"
)

type DocID string

type DocRef struct {
	ID    DocID
	Count uint32
}

type Result struct {
	ID            DocID
	UniqueMatches int
	TotalMatches  int
}

type SearchResult struct {
	Results           []Result
	TotalResultsCount int
	Timings           map[string]string
}

type Index interface {
	Insert(key string, id DocID) error
	Search(key string) ([]DocRef, error)
}

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

type Filter interface {
	Add(item []byte) bool
	Contains(item []byte) bool
}

type Engine interface {
	IndexDocument(ctx context.Context, docID DocID, content string) error
	SearchDocuments(ctx context.Context, query string, maxResults int) (*SearchResult, error)
}

func WordKeys(token string) ([]string, error) {
	return []string{token}, nil
}
