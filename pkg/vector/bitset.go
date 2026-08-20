package vector

import (
	"errors"
	"fmt"
)

var ErrOrdinalOutOfRange = errors.New("vector: ordinal is outside the bitset")

// BitSet is an immutable accepted-ordinal set. With returns a new BitSet and
// never changes snapshots held by concurrent readers.
type BitSet struct {
	size        uint32
	words       []uint64
	cardinality int
}

func NewBitSet(size uint32, accepted ...Ordinal) (BitSet, error) {
	set := BitSet{size: size, words: make([]uint64, wordCount(size))}
	for _, ord := range accepted {
		if uint32(ord) >= size {
			return BitSet{}, fmt.Errorf("%w: %d >= %d", ErrOrdinalOutOfRange, ord, size)
		}
		word, mask := bitLocation(ord)
		if set.words[word]&mask == 0 {
			set.words[word] |= mask
			set.cardinality++
		}
	}
	return set, nil
}

func NewFullBitSet(size uint32) BitSet {
	set := BitSet{size: size, words: make([]uint64, wordCount(size)), cardinality: int(size)}
	for i := range set.words {
		set.words[i] = ^uint64(0)
	}
	if remainder := size % 64; remainder != 0 {
		set.words[len(set.words)-1] = (uint64(1) << remainder) - 1
	}
	return set
}

func (s BitSet) Size() uint32 { return s.size }

func (s BitSet) Cardinality() int { return s.cardinality }

func (s BitSet) Contains(ord Ordinal) bool {
	if uint32(ord) >= s.size {
		return false
	}
	word, mask := bitLocation(ord)
	return s.words[word]&mask != 0
}

// With returns a snapshot with ord accepted or rejected. If the requested state
// is already present, the returned BitSet safely shares the immutable storage.
func (s BitSet) With(ord Ordinal, accepted bool) (BitSet, error) {
	if uint32(ord) >= s.size {
		return BitSet{}, fmt.Errorf("%w: %d >= %d", ErrOrdinalOutOfRange, ord, s.size)
	}
	if s.Contains(ord) == accepted {
		return s, nil
	}

	next := BitSet{
		size:        s.size,
		words:       append([]uint64(nil), s.words...),
		cardinality: s.cardinality,
	}
	word, mask := bitLocation(ord)
	if accepted {
		next.words[word] |= mask
		next.cardinality++
	} else {
		next.words[word] &^= mask
		next.cardinality--
	}
	return next, nil
}

func wordCount(size uint32) int {
	return int((uint64(size) + 63) / 64)
}

func bitLocation(ord Ordinal) (int, uint64) {
	value := uint32(ord)
	return int(value / 64), uint64(1) << (value % 64)
}

var _ AcceptSet = BitSet{}
