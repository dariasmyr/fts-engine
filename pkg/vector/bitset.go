package vector

import (
	"errors"
	"fmt"
)

var ErrOrdinalOutOfRange = errors.New("vector: ordinal is outside the bitset")

var (
	ErrBitSetShrink    = errors.New("vector: bitset cannot shrink")
	ErrConflictingBits = errors.New("vector: ordinal cannot be both accepted and rejected")
)

const bitSetBlockWords = 64

// BitSet is an immutable accepted-ordinal set. Snapshots share fixed-size word
// blocks; updates copy only the block table and changed blocks, never storage
// held by concurrent readers.
type BitSet struct {
	size        uint32
	blocks      [][]uint64
	cardinality int
}

func NewBitSet(size uint32, accepted ...Ordinal) (BitSet, error) {
	set := BitSet{size: size, blocks: makeBitSetBlocks(size)}
	for _, ord := range accepted {
		if uint32(ord) >= size {
			return BitSet{}, fmt.Errorf("%w: %d >= %d", ErrOrdinalOutOfRange, ord, size)
		}
		block, word, mask := bitLocation(ord)
		if set.blocks[block][word]&mask == 0 {
			set.blocks[block][word] |= mask
			set.cardinality++
		}
	}
	return set, nil
}

func NewFullBitSet(size uint32) BitSet {
	set := BitSet{size: size, blocks: makeBitSetBlocks(size), cardinality: int(size)}
	words := wordCount(size)
	for i := range words {
		block, word := wordBlock(i)
		set.blocks[block][word] = ^uint64(0)
	}
	if remainder := size % 64; remainder != 0 {
		block, word := wordBlock(words - 1)
		set.blocks[block][word] = (uint64(1) << remainder) - 1
	}
	return set
}

func (s BitSet) Size() uint32 { return s.size }

func (s BitSet) Cardinality() int { return s.cardinality }

func (s BitSet) Contains(ord Ordinal) bool {
	if uint32(ord) >= s.size {
		return false
	}
	block, word, mask := bitLocation(ord)
	return s.blocks[block][word]&mask != 0
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

	next := s.cloneStructure(s.size)
	block, word, mask := bitLocation(ord)
	next.blocks[block] = append([]uint64(nil), next.blocks[block]...)
	if accepted {
		next.blocks[block][word] |= mask
		next.cardinality++
	} else {
		next.blocks[block][word] &^= mask
		next.cardinality--
	}
	return next, nil
}

// WithChanges grows the ordinal domain and applies all changes with one copy.
// Newly added ordinals are rejected unless listed in accepted.
func (s BitSet) WithChanges(size uint32, accepted, rejected []Ordinal) (BitSet, error) {
	if size < s.size {
		return BitSet{}, fmt.Errorf("%w: %d < %d", ErrBitSetShrink, size, s.size)
	}
	changes := make(map[Ordinal]bool, len(accepted)+len(rejected))
	for _, ord := range accepted {
		if uint32(ord) >= size {
			return BitSet{}, fmt.Errorf("%w: %d >= %d", ErrOrdinalOutOfRange, ord, size)
		}
		changes[ord] = true
	}
	for _, ord := range rejected {
		if uint32(ord) >= size {
			return BitSet{}, fmt.Errorf("%w: %d >= %d", ErrOrdinalOutOfRange, ord, size)
		}
		if changes[ord] {
			return BitSet{}, fmt.Errorf("%w: %d", ErrConflictingBits, ord)
		}
		changes[ord] = false
	}

	next := s.cloneStructure(size)
	copied := make([]bool, len(next.blocks))
	for block := len(s.blocks); block < len(next.blocks); block++ {
		copied[block] = true
	}
	touched := make([]int, 0, min(len(changes), len(s.blocks)))
	for ord, value := range changes {
		block, word, mask := bitLocation(ord)
		current := next.blocks[block][word]&mask != 0
		if current != value && !copied[block] {
			copied[block] = true
			touched = append(touched, block)
		}
	}
	blockCopies := make([]uint64, len(touched)*bitSetBlockWords)
	for i, block := range touched {
		copyStart := i * bitSetBlockWords
		copyEnd := copyStart + bitSetBlockWords
		copy(blockCopies[copyStart:copyEnd], next.blocks[block])
		next.blocks[block] = blockCopies[copyStart:copyEnd]
	}
	for ord, value := range changes {
		block, word, mask := bitLocation(ord)
		current := next.blocks[block][word]&mask != 0
		if current == value {
			continue
		}
		if value {
			next.blocks[block][word] |= mask
			next.cardinality++
		} else {
			next.blocks[block][word] &^= mask
			next.cardinality--
		}
	}
	return next, nil
}

func wordCount(size uint32) int {
	return int((uint64(size) + 63) / 64)
}

func makeBitSetBlocks(size uint32) [][]uint64 {
	words := wordCount(size)
	blocks := make([][]uint64, (words+bitSetBlockWords-1)/bitSetBlockWords)
	storage := make([]uint64, len(blocks)*bitSetBlockWords)
	for i := range blocks {
		start := i * bitSetBlockWords
		blocks[i] = storage[start : start+bitSetBlockWords]
	}
	return blocks
}

func (s BitSet) cloneStructure(size uint32) BitSet {
	blockCount := (wordCount(size) + bitSetBlockWords - 1) / bitSetBlockWords
	blocks := make([][]uint64, blockCount)
	copy(blocks, s.blocks)
	storage := make([]uint64, (len(blocks)-len(s.blocks))*bitSetBlockWords)
	for i := len(s.blocks); i < len(blocks); i++ {
		start := (i - len(s.blocks)) * bitSetBlockWords
		blocks[i] = storage[start : start+bitSetBlockWords]
	}
	return BitSet{size: size, blocks: blocks, cardinality: s.cardinality}
}

func bitLocation(ord Ordinal) (int, int, uint64) {
	value := uint32(ord)
	wordIndex := int(value / 64)
	block, word := wordBlock(wordIndex)
	return block, word, uint64(1) << (value % 64)
}

func wordBlock(wordIndex int) (int, int) {
	return wordIndex / bitSetBlockWords, wordIndex % bitSetBlockWords
}

var _ AcceptSet = BitSet{}
