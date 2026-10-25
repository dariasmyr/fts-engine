package vector

import (
	"errors"
	"fmt"
	"math/bits"
)

var ErrOrdinalOutOfRange = errors.New("vector: ordinal is outside the bitset")

var (
	ErrBitSetShrink        = errors.New("vector: bitset cannot shrink")
	ErrConflictingOrdinals = errors.New("vector: ordinal cannot be both allowed and disallowed")
)

const bitSetBlockWords = 64

// BitSet is an immutable set of allowed ordinals. Snapshots share fixed-size word
// blocks; updates copy only the block table and changed blocks, never storage
// held by concurrent readers.
type BitSet struct {
	ordinalCount uint32
	blocks       [][]uint64
	allowedCount int
}

func NewBitSet(ordinalCount uint32, allowed ...Ordinal) (BitSet, error) {
	set := BitSet{ordinalCount: ordinalCount, blocks: makeBitSetBlocks(ordinalCount)}
	for _, ord := range allowed {
		if uint32(ord) >= ordinalCount {
			return BitSet{}, fmt.Errorf("%w: %d >= %d", ErrOrdinalOutOfRange, ord, ordinalCount)
		}
		block, word, mask := bitLocation(ord)
		if set.blocks[block][word]&mask == 0 {
			set.blocks[block][word] |= mask
			set.allowedCount++
		}
	}
	return set, nil
}

func NewFullBitSet(ordinalCount uint32) BitSet {
	set := BitSet{ordinalCount: ordinalCount, blocks: makeBitSetBlocks(ordinalCount), allowedCount: int(ordinalCount)}
	words := wordCount(ordinalCount)
	for i := range words {
		block, word := wordBlock(i)
		set.blocks[block][word] = ^uint64(0)
	}
	if remainder := ordinalCount % 64; remainder != 0 {
		block, word := wordBlock(words - 1)
		set.blocks[block][word] = (uint64(1) << remainder) - 1
	}
	return set
}

// NewBitSetFromWords restores a BitSet from 64-bit words. Each word stores the
// state of 64 consecutive ordinals. The values are copied, so changing words
// after this call does not change the returned BitSet. Unused bits in the last
// word must be zero.
func NewBitSetFromWords(ordinalCount uint32, words []uint64) (BitSet, error) {
	if len(words) != wordCount(ordinalCount) {
		return BitSet{}, fmt.Errorf("vector: bitset word count mismatch: got %d, want %d", len(words), wordCount(ordinalCount))
	}
	if len(words) > 0 && ordinalCount%64 != 0 {
		// With 130 ordinals, the last word may use only bits 0 and 1 for ordinals
		// 128 and 129.
		validMask := (uint64(1) << (ordinalCount % 64)) - 1
		// Bits outside validMask do not belong to the BitSet.
		if words[len(words)-1]&^validMask != 0 {
			return BitSet{}, errors.New("vector: bitset has non-zero trailing bits")
		}
	}
	// Create separate internal memory, then copy each input value into it.
	set := BitSet{ordinalCount: ordinalCount, blocks: makeBitSetBlocks(ordinalCount)}
	for i, wordValue := range words {
		block, word := wordBlock(i)
		set.blocks[block][word] = wordValue
		set.allowedCount += bits.OnesCount64(wordValue)
	}
	return set, nil
}

func (s BitSet) TotalOrdinalCount() uint32 { return s.ordinalCount }

func (s BitSet) AllowedOrdinalCount() int { return s.allowedCount }

// SnapshotWords returns a canonical copy ordered by increasing ordinal.
func (s BitSet) SnapshotWords() []uint64 {
	words := make([]uint64, wordCount(s.ordinalCount))
	for i := range words {
		block, word := wordBlock(i)
		words[i] = s.blocks[block][word]
	}
	return words
}

// Allows reports whether ord is marked as allowed. It returns false when ord is
// outside the range described by the BitSet.
func (s BitSet) Allows(ord Ordinal) bool {
	if uint32(ord) >= s.ordinalCount {
		return false
	}
	block, word, mask := bitLocation(ord)
	// Сhecking the block and word is safe because we already verified that ord is in range.
	// Return true if the bit is set, false otherwise.
	return s.blocks[block][word]&mask != 0
}

// With returns a snapshot with ord allowed or disallowed. If the requested state
// is already present, the returned BitSet safely shares the immutable storage.
func (s BitSet) With(ord Ordinal, allowed bool) (BitSet, error) {
	if uint32(ord) >= s.ordinalCount {
		return BitSet{}, fmt.Errorf("%w: %d >= %d", ErrOrdinalOutOfRange, ord, s.ordinalCount)
	}
	if s.Allows(ord) == allowed {
		return s, nil
	}

	next := s.cloneStructure(s.ordinalCount)
	block, word, mask := bitLocation(ord)
	next.blocks[block] = append([]uint64(nil), next.blocks[block]...)
	if allowed {
		next.blocks[block][word] |= mask
		next.allowedCount++
	} else {
		next.blocks[block][word] &^= mask
		next.allowedCount--
	}
	return next, nil
}

// WithChanges grows the number of ordinal positions and applies all changes with one copy.
// Newly added ordinals are disallowed unless listed in allowed.
func (s BitSet) WithChanges(ordinalCount uint32, allowed, disallowed []Ordinal) (BitSet, error) {
	if ordinalCount < s.ordinalCount {
		return BitSet{}, fmt.Errorf("%w: %d < %d", ErrBitSetShrink, ordinalCount, s.ordinalCount)
	}
	changes := make(map[Ordinal]bool, len(allowed)+len(disallowed))
	for _, ord := range allowed {
		if uint32(ord) >= ordinalCount {
			return BitSet{}, fmt.Errorf("%w: %d >= %d", ErrOrdinalOutOfRange, ord, ordinalCount)
		}
		changes[ord] = true
	}
	for _, ord := range disallowed {
		if uint32(ord) >= ordinalCount {
			return BitSet{}, fmt.Errorf("%w: %d >= %d", ErrOrdinalOutOfRange, ord, ordinalCount)
		}
		if changes[ord] {
			return BitSet{}, fmt.Errorf("%w: %d", ErrConflictingOrdinals, ord)
		}
		changes[ord] = false
	}

	next := s.cloneStructure(ordinalCount)
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
			next.allowedCount++
		} else {
			next.blocks[block][word] &^= mask
			next.allowedCount--
		}
	}
	return next, nil
}

func wordCount(ordinalCount uint32) int {
	return int((uint64(ordinalCount) + 63) / 64)
}

func makeBitSetBlocks(ordinalCount uint32) [][]uint64 {
	words := wordCount(ordinalCount)
	// One block stores 64 words, so we need (words + 63) / 64 blocks to cover all words.
	blocks := make([][]uint64, (words+bitSetBlockWords-1)/bitSetBlockWords)
	storage := make([]uint64, len(blocks)*bitSetBlockWords)
	for i := range blocks {
		start := i * bitSetBlockWords
		blocks[i] = storage[start : start+bitSetBlockWords]
	}
	return blocks
}

func (s BitSet) cloneStructure(ordinalCount uint32) BitSet {
	blockCount := (wordCount(ordinalCount) + bitSetBlockWords - 1) / bitSetBlockWords
	blocks := make([][]uint64, blockCount)
	copy(blocks, s.blocks)
	storage := make([]uint64, (len(blocks)-len(s.blocks))*bitSetBlockWords)
	for i := len(s.blocks); i < len(blocks); i++ {
		start := (i - len(s.blocks)) * bitSetBlockWords
		blocks[i] = storage[start : start+bitSetBlockWords]
	}
	return BitSet{ordinalCount: ordinalCount, blocks: blocks, allowedCount: s.allowedCount}
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
