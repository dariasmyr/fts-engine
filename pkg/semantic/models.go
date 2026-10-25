package semantic

// VectorID is a stable semantic vector identity. It survives compaction even
// when a vector moves to another segment ordinal.
type VectorID uint64

// SegmentID identifies one immutable physical segment.
type SegmentID uint64

// Revision identifies one logical mutation state of the index.
type Revision uint64
