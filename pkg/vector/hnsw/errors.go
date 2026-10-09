package hnsw

import "errors"

var (
	ErrInvalidSource        = errors.New("vector/hnsw: invalid prepared vector store")
	ErrInvalidSearchOptions = errors.New("vector/hnsw: invalid search options")
	errInvalidGraph         = errors.New("vector/hnsw: invalid graph")
	errCapacityExceeded     = errors.New("vector/hnsw: builder capacity exceeded")
	errVisitLimit           = errors.New("vector/hnsw: visit limit reached")
)
