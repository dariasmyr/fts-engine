package filters

import (
	"hash/fnv"
	"sync"
)

const (
	defaultBloomCapacity = uint64(1_000_000)
	defaultBloomHashes   = uint64(7)
	minBloomBits         = uint64(1024)
	bitsPerItem          = uint64(10)
)

type bloomFilter struct {
	mu   sync.RWMutex
	bits []uint64
	m    uint64
	k    uint64
}

func NewBloomFilter(cfg BloomConfig) Filter {
	capacity := cfg.Capacity
	if capacity == 0 {
		capacity = defaultBloomCapacity
	}

	hashes := cfg.Hashes
	if hashes == 0 {
		hashes = defaultBloomHashes
	}

	bitCount := capacity * bitsPerItem
	if bitCount < minBloomBits {
		bitCount = minBloomBits
	}
	bitCount = uint64(nextPow2Int(int(bitCount)))

	wordCount := (bitCount + 63) / 64
	return &bloomFilter{
		bits: make([]uint64, wordCount),
		m:    bitCount,
		k:    hashes,
	}
}

func (b *bloomFilter) Add(key string) {
	h1, h2 := bloomHashes(key)

	b.mu.Lock()
	defer b.mu.Unlock()
	for i := uint64(0); i < b.k; i++ {
		idx := (h1 + i*h2) % b.m
		word := idx / 64
		bit := idx % 64
		b.bits[word] |= uint64(1) << bit
	}
}

func (b *bloomFilter) MightContain(key string) bool {
	h1, h2 := bloomHashes(key)

	b.mu.RLock()
	defer b.mu.RUnlock()
	for i := uint64(0); i < b.k; i++ {
		idx := (h1 + i*h2) % b.m
		word := idx / 64
		bit := idx % 64
		if b.bits[word]&(uint64(1)<<bit) == 0 {
			return false
		}
	}
	return true
}

func bloomHashes(s string) (uint64, uint64) {
	h1 := fnv.New64a()
	_, _ = h1.Write([]byte(s))

	h2 := fnv.New64()
	_, _ = h2.Write([]byte(s))
	sum2 := h2.Sum64()
	if sum2%2 == 0 {
		sum2++
	}

	return h1.Sum64(), sum2
}

func nextPow2Int(v int) int {
	if v <= 1 {
		return 1
	}
	n := 1
	for n < v {
		n <<= 1
	}
	return n
}
