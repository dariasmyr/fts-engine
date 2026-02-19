package filters

import (
	"hash/fnv"
	"sync"
)

const (
	defaultRibbonWidthFilter = uint64(8)
	defaultRibbonBitsFilter  = uint64(1 << 20)
)

type ribbonFilter struct {
	mu       sync.RWMutex
	bits     []uint64
	bitCount uint64
	width    uint64
}

func NewRibbonFilter(cfg RibbonConfig) Filter {
	bitCount := cfg.Bits
	if bitCount == 0 {
		bitCount = defaultRibbonBitsFilter
	}
	if bitCount < minBloomBits {
		bitCount = minBloomBits
	}
	bitCount = uint64(nextPow2Int(int(bitCount)))

	width := cfg.Width
	if width == 0 {
		width = defaultRibbonWidthFilter
	}
	if width >= bitCount {
		width = defaultRibbonWidthFilter
	}

	wordCount := (bitCount + 63) / 64
	return &ribbonFilter{
		bits:     make([]uint64, wordCount),
		bitCount: bitCount,
		width:    width,
	}
}

func (r *ribbonFilter) Add(key string) {
	start := r.start(key)

	r.mu.Lock()
	defer r.mu.Unlock()
	for i := uint64(0); i < r.width; i++ {
		idx := start + i
		word := idx / 64
		bit := idx % 64
		r.bits[word] |= uint64(1) << bit
	}
}

func (r *ribbonFilter) MightContain(key string) bool {
	start := r.start(key)

	r.mu.RLock()
	defer r.mu.RUnlock()
	for i := uint64(0); i < r.width; i++ {
		idx := start + i
		word := idx / 64
		bit := idx % 64
		if r.bits[word]&(uint64(1)<<bit) == 0 {
			return false
		}
	}
	return true
}

func (r *ribbonFilter) start(key string) uint64 {
	span := r.bitCount - r.width + 1
	return ribbonHash(key) % span
}

func ribbonHash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
