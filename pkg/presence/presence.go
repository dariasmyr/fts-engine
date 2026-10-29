// Package presence provides immutable exact-term posting indexes that store only
// document ordinals. It is intended for embedded stores that own document IDs
// and filtering. It stores no frequencies, positions, analyzer metadata,
// tombstones or document registry. Use posting.Intersect to combine its postings
// with other terms or external candidate ordinals.
//
// The API and FPR1 format are experimental and separate from FTSE segments.
// FPR1 readers and writers remain paired within this major module version, but
// the format has no long-term backward-compatibility guarantee until promotion
// from experimental status. An incompatible promoted format must use a new magic
// value rather than silently changing FPR1.
package presence

const (
	magic          = "FPR1"
	footerSize     = 24
	blockSize      = 128
	blockEntrySize = 12 // last ordinal, relative offset, byte length
)
