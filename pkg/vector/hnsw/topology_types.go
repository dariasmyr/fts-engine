package hnsw

// NodeOrdinal is a dense graph-local node number. It is intentionally distinct
// from vector.Ordinal even when one graph node maps to one vector row.
type NodeOrdinal uint32
