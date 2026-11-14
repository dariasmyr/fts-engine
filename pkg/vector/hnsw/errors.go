package hnsw

import "errors"

var (
	ErrInvalidSource      = errors.New("vector/hnsw: invalid prepared vector store")
	ErrInvalidQueryConfig = errors.New("vector/hnsw: invalid query configuration")
	errInvalidGraph       = errors.New("vector/hnsw: invalid graph")
	errCapacityExceeded   = errors.New("vector/hnsw: builder capacity exceeded")
	errConnectivityRepair = errors.New("vector/hnsw: cannot safely repair graph connectivity")
	errVisitLimit         = errors.New("vector/hnsw: visit limit reached")
)
