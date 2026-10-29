package posting

import "context"

// OrdinalIterator enumerates strictly increasing document ordinals. It starts
// before the first item. Advance moves to the first ordinal >= target, retaining
// the current item if it already satisfies target. Exhaustion is permanent.
// Iterators are owned by their consumer and must be closed on every return path.
type OrdinalIterator interface {
	Next() bool
	Advance(target Ord) bool
	Ord() Ord
	Err() error
	Close() error
}

// FrequencyIterator extends OrdinalIterator with the term frequency at the current ordinal.
type FrequencyIterator interface {
	OrdinalIterator
	Frequency() uint32
}

// FrequencyPostingOpener opens exact-term posting iterators that expose each
// document ordinal and its term frequency. The returned count is the exact,
// unfiltered document frequency. Missing terms return an empty iterator and a
// zero count without an error.
type FrequencyPostingOpener interface {
	OpenFrequencyPostings(context.Context, string) (FrequencyIterator, uint64, error)
}

// OrdinalPostingOpener opens ordinal-only iterators for exact terms. Frequencies
// may remain stored by the underlying index but are not exposed through the
// returned iterator. The returned count is the exact document frequency.
// Missing terms return an empty iterator and a zero count without an error.
type OrdinalPostingOpener interface {
	OpenOrdinalPostings(context.Context, string) (OrdinalIterator, uint64, error)
}
