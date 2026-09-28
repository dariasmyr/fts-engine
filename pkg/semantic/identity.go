package semantic

// VectorID is the stable internal identity of one stored embedding vector.
// It is allocated monotonically and is not reused after replacement or
// deletion. It is separate from chunk.ID: the former identifies an index row,
// while the latter identifies the source chunk returned to the caller.
type VectorID uint64

type ComponentID uint64

const MutableHeadID ComponentID = 1
