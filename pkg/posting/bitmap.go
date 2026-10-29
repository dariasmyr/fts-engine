package posting

import "math/bits"

// NewBitmapIterator adapts a dense bitmap whose bit number is an Ord. It is
// suitable for passing an external store's candidate bitmap to
// IntersectWithIterator or FilterPostingsWithIterator. Words are ordered low to
// high and the least-significant bit is the first ordinal in a word. The caller
// must not mutate words until Close.
func NewBitmapIterator(words []uint64) OrdinalIterator {
	return &bitmapIterator{words: words}
}

type bitmapIterator struct {
	words []uint64
	ord   Ord
	valid bool
	done  bool
}

func (i *bitmapIterator) Next() bool {
	if i.done {
		return false
	}
	if !i.valid {
		return i.Advance(0)
	}
	if i.ord == ^Ord(0) {
		i.done = true
		return false
	}
	return i.advanceFrom(i.ord + 1)
}

func (i *bitmapIterator) Advance(target Ord) bool {
	if i.done {
		return false
	}
	if i.valid && i.ord >= target {
		return true
	}
	return i.advanceFrom(target)
}

func (i *bitmapIterator) advanceFrom(target Ord) bool {
	wordIndex := int(uint64(target) / 64)
	if wordIndex >= len(i.words) {
		i.done = true
		return false
	}
	bitOffset := target % 64
	mask := ^uint64(0) << uint64(bitOffset)
	value := i.words[wordIndex] & mask
	for {
		if value != 0 {
			startWordIndex := uint64(wordIndex) * 64
			// Bits below target were masked out, so the lowest set bit is target
			// itself when present, or the next present ordinal when it is absent.
			bitPosition := uint64(bits.TrailingZeros64(value))
			ord := startWordIndex + bitPosition
			if ord > uint64(^Ord(0)) {
				i.done = true
				return false
			}
			i.ord = Ord(ord)
			i.valid = true
			return true
		}
		wordIndex++
		if wordIndex >= len(i.words) {
			i.done = true
			return false
		}
		value = i.words[wordIndex]
	}
}

func (i *bitmapIterator) Ord() Ord     { return i.ord }
func (i *bitmapIterator) Err() error   { return nil }
func (i *bitmapIterator) Close() error { i.done, i.words = true, nil; return nil }
