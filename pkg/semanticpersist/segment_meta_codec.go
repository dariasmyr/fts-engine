package semanticpersist

import (
	"math"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

const (
	segmentMetaMagic   = "VSMT"
	segmentMetaVersion = uint16(1)
)

func encodeSegmentMeta(checkpoint semantic.Checkpoint, vectors fileReference, limits Limits) ([]byte, fileReference, error) {
	if len(checkpoint.VectorIDs) > limits.MaxVectors {
		return nil, fileReference{}, ErrLimitExceeded
	}
	for i, id := range checkpoint.VectorIDs {
		if id == 0 || i > 0 && checkpoint.VectorIDs[i-1] >= id {
			return nil, fileReference{}, ErrCorrupt
		}
	}
	stats := checkpoint.DuplicateStatistics
	if stats.VectorRows < 0 || stats.UniqueVectors < 0 || stats.DuplicateRows < 0 || stats.DuplicateGroups < 0 || stats.MaxFanOut < 0 {
		return nil, fileReference{}, ErrCorrupt
	}
	words := checkpoint.Live.SnapshotWords()
	e := newEncoder(segmentMetaMagic, segmentMetaVersion, limits.MaxFileBytes)
	e.u32(uint32(len(checkpoint.VectorIDs)))
	e.u32(checkpoint.Live.TotalOrdinalCount())
	e.u32(uint32(len(words)))
	e.u32(0)
	e.u64(vectors.Size)
	e.raw(vectors.SHA256[:])
	e.u64(uint64(stats.VectorRows))
	e.u64(uint64(stats.UniqueVectors))
	e.u64(uint64(stats.DuplicateRows))
	e.u64(uint64(stats.DuplicateGroups))
	e.u64(uint64(stats.MaxFanOut))
	for _, id := range checkpoint.VectorIDs {
		e.u64(uint64(id))
	}
	for _, word := range words {
		e.u64(word)
	}
	return e.finish()
}

func decodeSegmentMeta(data []byte, limits Limits) (decodedSegmentMeta, error) {
	d, err := newDecoder(data, segmentMetaMagic, segmentMetaVersion, limits)
	if err != nil {
		return decodedSegmentMeta{}, err
	}
	count := int(d.u32())
	liveSize := d.u32()
	wordCount := int(d.u32())
	if d.u32() != 0 || count < 0 || count > limits.MaxVectors || liveSize != uint32(count) || wordCount != (count+63)/64 {
		return decodedSegmentMeta{}, ErrCorrupt
	}
	vectors := fileReference{Size: d.u64()}
	copy(vectors.SHA256[:], d.take(len(vectors.SHA256)))
	stats := semantic.DuplicateStatistics{
		VectorRows: decodeBoundedInt(d), UniqueVectors: decodeBoundedInt(d), DuplicateRows: decodeBoundedInt(d), DuplicateGroups: decodeBoundedInt(d), MaxFanOut: decodeBoundedInt(d),
	}
	requiredValues := uint64(count+wordCount) * 8
	if requiredValues > uint64(d.remaining()) || requiredValues != uint64(d.remaining()) {
		return decodedSegmentMeta{}, ErrCorrupt
	}
	ids := make([]semantic.VectorID, count)
	for i := range ids {
		ids[i] = semantic.VectorID(d.u64())
		if ids[i] == 0 || i > 0 && ids[i-1] >= ids[i] {
			return decodedSegmentMeta{}, ErrCorrupt
		}
	}
	words := make([]uint64, wordCount)
	for i := range words {
		words[i] = d.u64()
	}
	if err := d.done(); err != nil {
		return decodedSegmentMeta{}, err
	}
	if _, err := vector.NewBitSetFromWords(liveSize, words); err != nil {
		return decodedSegmentMeta{}, ErrCorrupt
	}
	return decodedSegmentMeta{VectorIDs: ids, Live: words, LiveSize: liveSize, Vectors: vectors, DuplicateStatistics: stats}, nil
}

func decodeBoundedInt(d *decoder) int {
	value := d.u64()
	if value > uint64(math.MaxInt) {
		d.err = ErrLimitExceeded
		return 0
	}
	return int(value)
}
