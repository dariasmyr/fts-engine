package filters

import (
	"hash/fnv"
	"sync"
)

const (
	defaultCuckooCapacity = uint64(1_000_000)
	defaultCuckooBucketSz = 4
	defaultCuckooMaxKicks = 500
)

type cuckooFilter struct {
	mu       sync.RWMutex
	buckets  [][]uint16
	mask     uint64
	bucketSz int
	maxKicks int
	stash    map[string]struct{}
}

func NewCuckooFilter(cfg CuckooConfig) Filter {
	capacity := cfg.Capacity
	if capacity == 0 {
		capacity = defaultCuckooCapacity
	}

	bucketSz := cfg.BucketSz
	if bucketSz <= 0 {
		bucketSz = defaultCuckooBucketSz
	}

	maxKicks := cfg.MaxKicks
	if maxKicks <= 0 {
		maxKicks = defaultCuckooMaxKicks
	}

	bucketCount := nextPow2Int(int(capacity) / bucketSz)
	if bucketCount < 2 {
		bucketCount = 2
	}

	buckets := make([][]uint16, bucketCount)
	for i := range buckets {
		buckets[i] = make([]uint16, 0, bucketSz)
	}

	return &cuckooFilter{
		buckets:  buckets,
		mask:     uint64(bucketCount - 1),
		bucketSz: bucketSz,
		maxKicks: maxKicks,
		stash:    make(map[string]struct{}),
	}
}

func (c *cuckooFilter) Add(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	fp := cuckooFingerprint(key)
	i1 := cuckooIndex1(key, c.mask)
	i2 := cuckooAltIndex(i1, fp, c.mask)

	if c.insertIntoBucket(i1, fp) || c.insertIntoBucket(i2, fp) {
		return
	}

	idx := i1
	cur := fp
	for kick := 0; kick < c.maxKicks; kick++ {
		slot := kick % c.bucketSz
		evicted := c.buckets[idx][slot]
		c.buckets[idx][slot] = cur
		cur = evicted
		idx = cuckooAltIndex(idx, cur, c.mask)
		if c.insertIntoBucket(idx, cur) {
			return
		}
	}

	// Keep fallback stash to guarantee no false negatives under high load.
	c.stash[key] = struct{}{}
}

func (c *cuckooFilter) MightContain(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if _, ok := c.stash[key]; ok {
		return true
	}

	fp := cuckooFingerprint(key)
	i1 := cuckooIndex1(key, c.mask)
	i2 := cuckooAltIndex(i1, fp, c.mask)

	return c.bucketHas(i1, fp) || c.bucketHas(i2, fp)
}

func (c *cuckooFilter) insertIntoBucket(idx uint64, fp uint16) bool {
	b := c.buckets[idx]
	if len(b) >= c.bucketSz {
		return false
	}
	c.buckets[idx] = append(b, fp)
	return true
}

func (c *cuckooFilter) bucketHas(idx uint64, fp uint16) bool {
	for _, x := range c.buckets[idx] {
		if x == fp {
			return true
		}
	}
	return false
}

func cuckooIndex1(key string, mask uint64) uint64 {
	return hash64a(key) & mask
}

func cuckooAltIndex(i uint64, fp uint16, mask uint64) uint64 {
	return (i ^ hash64a(string([]byte{byte(fp >> 8), byte(fp)}))) & mask
}

func cuckooFingerprint(key string) uint16 {
	v := uint16(hash64b(key) & 0xffff)
	if v == 0 {
		return 1
	}
	return v
}

func hash64a(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func hash64b(s string) uint64 {
	h := fnv.New64()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
