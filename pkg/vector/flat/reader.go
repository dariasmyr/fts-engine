package flat

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"slices"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// Reader is an immutable exact-search segment backed by a contiguous matrix.
type Reader struct {
	space  vector.Space
	maxK   int
	values []float32
}

func newReader(space vector.Space, maxK int, values []float32) *Reader {
	return &Reader{space: space, maxK: maxK, values: values}
}

func (r *Reader) Search(ctx context.Context, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	return searchExact(ctx, r.space, r.values, r.maxK, query, k, options)
}

func (r *Reader) Len() int { return len(r.values) / r.space.Dimensions() }

func (r *Reader) Dimensions() int { return r.space.Dimensions() }

func (r *Reader) Metric() vector.Metric { return r.space.Metric() }

func (r *Reader) Normalization() vector.Normalization { return r.space.Normalization() }

func (r *Reader) MaxK() int { return r.maxK }

// Vector returns a copy of one prepared vector row.
func (r *Reader) Vector(ord vector.Ordinal) ([]float32, bool) {
	value, ok := r.vectorView(ord)
	if !ok {
		return nil, false
	}
	return append([]float32(nil), value...), true
}

func (r *Reader) vectorView(ord vector.Ordinal) ([]float32, bool) {
	if uint32(ord) >= uint32(r.Len()) {
		return nil, false
	}
	start := int(ord) * r.Dimensions()
	return r.values[start : start+r.Dimensions()], true
}

// Close is present for parity with future file-backed readers.
func (r *Reader) Close() error { return nil }

type DuplicateStats struct {
	VectorRows      int
	UniqueVectors   int
	DuplicateRows   int
	DuplicateGroups int
	MaxFanOut       int
}

// ExactDuplicateStats groups exact prepared float32 rows without coalescing
// physical storage. namespace should identify the embedding space and format.
func (r *Reader) ExactDuplicateStats(namespace string) DuplicateStats {
	type group struct {
		ordinal vector.Ordinal
		count   int
	}
	buckets := make(map[[sha256.Size]byte][]*group, r.Len())
	groups := make([]*group, 0, r.Len())
	var encoded [4]byte
	for row := range r.Len() {
		vectorValue, _ := r.vectorView(vector.Ordinal(row))
		hash := sha256.New()
		_, _ = hash.Write([]byte(namespace))
		for _, component := range vectorValue {
			binary.LittleEndian.PutUint32(encoded[:], math.Float32bits(component))
			_, _ = hash.Write(encoded[:])
		}
		var sum [sha256.Size]byte
		copy(sum[:], hash.Sum(nil))
		var matched *group
		for _, candidate := range buckets[sum] {
			other, _ := r.vectorView(candidate.ordinal)
			if slices.EqualFunc(vectorValue, other, func(a, b float32) bool {
				return math.Float32bits(a) == math.Float32bits(b)
			}) {
				matched = candidate
				break
			}
		}
		if matched == nil {
			matched = &group{ordinal: vector.Ordinal(row)}
			buckets[sum] = append(buckets[sum], matched)
			groups = append(groups, matched)
		}
		matched.count++
	}
	stats := DuplicateStats{VectorRows: r.Len(), UniqueVectors: len(groups)}
	for _, group := range groups {
		if group.count > 1 {
			stats.DuplicateGroups++
			stats.DuplicateRows += group.count - 1
		}
		stats.MaxFanOut = max(stats.MaxFanOut, group.count)
	}
	return stats
}
