package flat

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/internal/contextcheck"
)

// VectorSource is immutable prepared vector storage backed by a flat matrix.
type VectorSource struct {
	space  vector.Space
	values []float32
}

func newVectorSource(space vector.Space, values []float32) *VectorSource {
	return &VectorSource{space: space, values: values}
}

func (s *VectorSource) Len() int { return len(s.values) / s.space.Dimensions() }

func (s *VectorSource) Dimensions() int { return s.space.Dimensions() }

func (s *VectorSource) Metric() vector.Metric { return s.space.Metric() }

func (s *VectorSource) Normalization() vector.Normalization { return s.space.Normalization() }

// ReadVectorInto copies one prepared row into dst without exposing source storage.
func (s *VectorSource) ReadVectorInto(ctx context.Context, ord vector.Ordinal, dst []float32) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(dst) != s.Dimensions() {
		return fmt.Errorf("%w: got %d, want %d", vector.ErrDimensionMismatch, len(dst), s.Dimensions())
	}
	value, ok := s.vectorView(ord)
	if !ok {
		return fmt.Errorf("%w: %d", vector.ErrOrdinalOutOfRange, ord)
	}
	copy(dst, value)
	return nil
}

// Vector returns a copy of one prepared vector row.
func (s *VectorSource) Vector(ord vector.Ordinal) ([]float32, bool) {
	value, ok := s.vectorView(ord)
	if !ok {
		return nil, false
	}
	return append([]float32(nil), value...), true
}

func (s *VectorSource) vectorView(ord vector.Ordinal) ([]float32, bool) {
	if uint32(ord) >= uint32(s.Len()) {
		return nil, false
	}
	start := int(ord) * s.Dimensions()
	return s.values[start : start+s.Dimensions()], true
}

type DuplicateStats struct {
	VectorRows      int
	UniqueVectors   int
	DuplicateRows   int
	DuplicateGroups int
	MaxFanOut       int
}

// ExactDuplicateStats groups exact prepared float32 rows without coalescing
// physical storage. namespace should identify the embedding space and format.
func (s *VectorSource) ExactDuplicateStats(namespace string) DuplicateStats {
	stats, _ := s.ExactDuplicateStatsContext(context.Background(), namespace)
	return stats
}

// ExactDuplicateStatsContext is ExactDuplicateStats with cancellation for the
// full row scan.
func (s *VectorSource) ExactDuplicateStatsContext(ctx context.Context, namespace string) (DuplicateStats, error) {
	if ctx == nil {
		return DuplicateStats{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return DuplicateStats{}, err
	}
	type group struct {
		ordinal vector.Ordinal
		count   int
	}
	buckets := make(map[[sha256.Size]byte][]*group, s.Len())
	groups := make([]*group, 0, s.Len())
	var encoded [4]byte
	for row := range s.Len() {
		if err := contextcheck.PeriodicError(ctx, row); err != nil {
			return DuplicateStats{}, err
		}
		vectorValue, _ := s.vectorView(vector.Ordinal(row))
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
			other, _ := s.vectorView(candidate.ordinal)
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
	stats := DuplicateStats{VectorRows: s.Len(), UniqueVectors: len(groups)}
	for _, group := range groups {
		if group.count > 1 {
			stats.DuplicateGroups++
			stats.DuplicateRows += group.count - 1
		}
		stats.MaxFanOut = max(stats.MaxFanOut, group.count)
	}
	if err := ctx.Err(); err != nil {
		return DuplicateStats{}, err
	}
	return stats, nil
}

var _ vector.PreparedVectorSource = (*VectorSource)(nil)
