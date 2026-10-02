package semantic

import "github.com/dariasmyr/fts-engine/pkg/chunk"

type Statistics struct {
	Documents       int
	PhysicalVectors int
	LiveVectors     int
	StaleVectors    int
	// MaxAllocatedVectorID never decreases after replacement, deletion, or compaction.
	MaxAllocatedVectorID uint64
}

// VectorRow connects an internal vector identity with the source chunk it
// represents. Segments store these rows by local ordinal, so the ordinal
// may change when segments are compacted while VectorID and Chunk remain the
// same. For example, VectorID 42 can move from ordinal 5 to ordinal 0 without
// changing which document chunk it represents.
type VectorRow struct {
	VectorID uint64
	Chunk    chunk.Ref
}
