package posting

// Ord is a dense document ordinal within one index generation.
type Ord uint32

// Posting records the term frequency for one document ordinal.
type Posting struct {
	Ord   Ord
	Count uint32
}

// PositionalPosting associates an ordinal with sorted term positions. Positions
// may share backing storage with an index and must be treated as read-only.
type PositionalPosting struct {
	Ord       Ord
	Positions []uint32
}
