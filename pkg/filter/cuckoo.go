package filter

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
)

type Bucket struct {
	slots []uint16
}

type CuckooFilter struct {
	buckets         []Bucket // array of buckets
	bucketSize      int      // number of slots per bucket
	maxKicks        int      // max number of evictions during insert
	fingerprintMask uint16
}

type cuckooSnapshot struct {
	BucketSize int
	MaxKicks   int
	Buckets    [][]uint16
}

func NewCuckooFilter(bucketCount int, bucketSize int, maxKicks int) *CuckooFilter {
	mask := ^uint16(0)

	buckets := make([]Bucket, bucketCount)

	for i := range buckets {
		buckets[i] = Bucket{
			slots: make([]uint16, 0, bucketSize),
		}
	}

	return &CuckooFilter{
		buckets:         buckets,
		bucketSize:      bucketSize,
		maxKicks:        maxKicks,
		fingerprintMask: mask,
	}
}

func (cf *CuckooFilter) Serialize(w io.Writer) error {
	buckets := make([][]uint16, 0, len(cf.buckets))
	for _, bucket := range cf.buckets {
		buckets = append(buckets, append([]uint16(nil), bucket.slots...))
	}

	snapshot := cuckooSnapshot{
		BucketSize: cf.bucketSize,
		MaxKicks:   cf.maxKicks,
		Buckets:    buckets,
	}

	if err := gob.NewEncoder(w).Encode(snapshot); err != nil {
		return fmt.Errorf("cuckoo: serialize: %w", err)
	}

	return nil
}

func LoadCuckooFilter(r io.Reader) (*CuckooFilter, error) {
	var snap cuckooSnapshot
	if err := gob.NewDecoder(r).Decode(&snap); err != nil {
		return nil, fmt.Errorf("cuckoo: load: %w", err)
	}

	cf := NewCuckooFilter(len(snap.Buckets), snap.BucketSize, snap.MaxKicks)
	for i := range snap.Buckets {
		cf.buckets[i].slots = append(cf.buckets[i].slots[:0], snap.Buckets[i]...)
	}

	return cf, nil
}

func (cf *CuckooFilter) fingerprint(key []byte) uint16 {
	h := fnv.New32a()
	h.Write(key)

	fp := uint16(h.Sum32()) & cf.fingerprintMask

	// zero is skipped and reserved for empty slots
	if fp == 0 {
		return 1
	}

	return fp
}

// index1 computes the primary bucket index for a key.
func (cf *CuckooFilter) index1(key []byte) uint32 {
	h := fnv.New32a()
	h.Write(key)

	return h.Sum32() % uint32(len(cf.buckets))
}

// index2 computes the alternate bucket index using XOR with fingerprint hash.
func (cf *CuckooFilter) index2(i1 uint32, fp uint16) uint32 {
	h := uint32(fp) * 0x5bd1e995
	return (i1 ^ h) % uint32(len(cf.buckets))
}

// findIndexes calculates fingerprint and both candidate bucket indexes for a key.
func (cf *CuckooFilter) findIndexes(key []byte) (fp uint16, i1, i2 uint32) {
	fp = cf.fingerprint(key)
	i1 = cf.index1(key)
	i2 = cf.index2(i1, fp)

	return fp, i1, i2
}

// Contains checks whether a key is probably present in the filter.
func (cf *CuckooFilter) Contains(key []byte) bool {
	fp, i1, i2 := cf.findIndexes(key)

	// false means the key is "definitely not" present.
	return cf.buckets[i1].has(fp) || cf.buckets[i2].has(fp)
}

// Add attempts to insert a key into the filter.
func (cf *CuckooFilter) Add(key []byte) bool {
	fp, i1, i2 := cf.findIndexes(key)

	// try direct insertion into either bucket
	if cf.buckets[i1].insert(fp) || cf.buckets[i2].insert(fp) {
		return true
	}

	// start cuckoo eviction chain
	i := i1
	for n := 0; n < cf.maxKicks; n++ {
		fp = cf.buckets[i].swapRandom(fp)
		i = cf.index2(i, fp)

		if cf.buckets[i].insert(fp) {
			return true
		}
	}

	// Insert failed (> maxKicks)
	return false
}

// has checks if fingerprint exists in the bucket.
func (b *Bucket) has(fp uint16) bool {
	for _, v := range b.slots {
		if v == fp {
			return true
		}
	}
	return false
}

// insert attempts to add fingerprint into bucket if space is available.
func (b *Bucket) insert(fp uint16) bool {
	if len(b.slots) < cap(b.slots) {
		b.slots = append(b.slots, fp)
		return true
	}
	return false
}

// swapRandom randomly evicts one fingerprint and replaces it with new one.
func (b *Bucket) swapRandom(fp uint16) uint16 {
	i := rand.Intn(len(b.slots))
	old := b.slots[i]
	b.slots[i] = fp

	// returns the evicted fingerprint.
	return old
}
