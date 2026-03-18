package filter

import (
	"encoding/binary"
	"hash/fnv"
)

type BloomFilter struct {
	m         uint64 // number of bytes in bitset
	k         uint64 // numbed of hash-funcs
	bitset    []uint64
	hashFuncs []func([]byte) uint64
}

// NewBloomFilter created new Bloom filter
// expectedItems — expected number of words
func NewBloomFilter(expectedItems uint64, bitsPerItem uint64, k uint64) *BloomFilter {
	m := expectedItems * bitsPerItem

	wordCount := (m + 63) / 64

	bitset := make([]uint64, wordCount)

	hashFuncs := make([]func([]byte) uint64, k)

	// Initiate every hash-funcs based on FNV-1a and seed.
	for seed := uint64(0); seed < k; seed++ {
		hashFuncs[seed] = func(data []byte) uint64 {
			h := fnv.New64a() // new 64-bit FNV-1a hasher [web:11]

			// Write seed as 8 bytes into hasher
			seedBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(seedBytes, seed)
			h.Write(seedBytes)

			h.Write(data)

			// Return 64-bit hash result
			return h.Sum64()
		}
	}

	return &BloomFilter{
		m:         m,
		k:         k,
		bitset:    bitset,
		hashFuncs: hashFuncs,
	}
}

// bitLocation define word index in bf.bitset and index of a byte in a word
func (bf *BloomFilter) bitLocation(hashValue uint64) (uint64, uint64) {
	// Bring hash value to the range [0, m) by taking module of m.
	pos := hashValue % bf.m

	wordIndex := pos / 64
	bitIndex := pos % 64
	mask := uint64(1) << bitIndex

	return wordIndex, mask
}

// setBit looks for global position of a bit and set to 1.
func (bf *BloomFilter) setBit(hashValue uint64) {

	// Find word position and bitmask
	wordIndex, mask := bf.bitLocation(hashValue)

	// Set bit with mask
	bf.bitset[wordIndex] |= mask
}

// isBitSet looks for global position of a bit and checks if it is set to 1.
func (bf *BloomFilter) isBitSet(hashValue uint64) bool {

	wordIndex, mask := bf.bitLocation(hashValue)

	// Check if bit set
	return (bf.bitset[wordIndex] & mask) != 0
}

// Add adds element to filter
func (bf *BloomFilter) Add(item []byte) {
	for i := uint64(0); i < bf.k; i++ {
		h := bf.hashFuncs[i]
		hashValue := h(item)

		bf.setBit(hashValue)
	}
}

// Contains checks is element "probably exists", or "certainly does not exist" in the filter
func (bf *BloomFilter) Contains(item []byte) bool {
	for i := uint64(0); i < bf.k; i++ {
		h := bf.hashFuncs[i]
		hashValue := h(item)

		if !bf.isBitSet(hashValue) {
			// element certainly does not exist
			return false
		}
	}

	// Element probably exists
	return true
}
