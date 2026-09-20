package flat

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestCodecRoundTripAndDeterministicBytes(t *testing.T) {
	for _, metric := range []vector.Metric{vector.MetricL2Squared, vector.MetricCosine} {
		t.Run(metric.String(), func(t *testing.T) {
			idx, err := New(Config{Dimensions: 3, Metric: metric, MaxVectors: 10, MaxK: 5})
			if err != nil {
				t.Fatal(err)
			}
			vectors := [][]float32{{1, 2, 3}, {3, 2, 1}, {1, 2, 3}}
			if _, err := idx.AppendBatch(vectors); err != nil {
				t.Fatal(err)
			}
			frozen := idx.Freeze()
			first, firstMetadata, err := Marshal(frozen)
			if err != nil {
				t.Fatal(err)
			}
			second, secondMetadata, err := Marshal(frozen)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(first, second) || firstMetadata != secondMetadata {
				t.Fatal("codec output is not deterministic")
			}
			opened, openedMetadata, err := Open(bytes.NewReader(first), DefaultCodecLimits())
			if err != nil {
				t.Fatal(err)
			}
			if openedMetadata != firstMetadata || opened.Len() != 3 || opened.Dimensions() != 3 || opened.Metric() != metric {
				t.Fatalf("opened metadata/reader mismatch: %+v %+v", openedMetadata, opened)
			}
			want, err := frozen.Search(context.Background(), []float32{1, 2, 3}, 3, vector.SearchOptions{})
			if err != nil {
				t.Fatal(err)
			}
			got, err := opened.Search(context.Background(), []float32{1, 2, 3}, 3, vector.SearchOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got.Hits, want.Hits) {
				t.Fatalf("round-trip hits = %+v, want %+v", got.Hits, want.Hits)
			}
			stats := opened.VectorSource().ExactDuplicateStats("space/test/v1")
			if stats.VectorRows != 3 || stats.UniqueVectors != 2 || stats.DuplicateRows != 1 || stats.DuplicateGroups != 1 || stats.MaxFanOut != 2 {
				t.Fatalf("duplicate stats = %+v", stats)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := opened.VectorSource().ExactDuplicateStatsContext(ctx, "space/test/v1"); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled duplicate scan error = %v", err)
			}
		})
	}
}

func TestFreezeIsIndependentFromMutableIndex(t *testing.T) {
	idx := newTestIndex(t, 10, 10)
	if _, err := idx.AppendBatch([][]float32{{1, 0}}); err != nil {
		t.Fatal(err)
	}
	frozen := idx.Freeze()
	if _, err := idx.AppendBatch([][]float32{{2, 0}}); err != nil {
		t.Fatal(err)
	}
	if frozen.Len() != 1 || idx.Len() != 2 {
		t.Fatalf("frozen/mutable lengths = %d/%d", frozen.Len(), idx.Len())
	}
}

func TestReaderVectorReturnsIndependentCopy(t *testing.T) {
	idx := newTestIndex(t, 2, 2)
	if _, err := idx.AppendBatch([][]float32{{1, 2}}); err != nil {
		t.Fatal(err)
	}
	reader := idx.Freeze()
	value, ok := reader.VectorSource().Vector(0)
	if !ok {
		t.Fatal("Vector(0) not found")
	}
	value[0] = 99
	again, _ := reader.VectorSource().Vector(0)
	if again[0] != 1 {
		t.Fatalf("reader backing storage was mutated: %v", again)
	}
}

func TestCodecRejectsCorruptionLimitsAndTrailingData(t *testing.T) {
	idx := newTestIndex(t, 10, 10)
	if _, err := idx.AppendBatch([][]float32{{1, 2}, {3, 4}}); err != nil {
		t.Fatal(err)
	}
	data, _, err := Marshal(idx.Freeze())
	if err != nil {
		t.Fatal(err)
	}

	for _, size := range []int{0, 3, codecHeaderSize - 1, len(data) - 1} {
		if _, _, err := Open(bytes.NewReader(data[:size]), DefaultCodecLimits()); err == nil {
			t.Fatalf("Open(truncated to %d) error = nil", size)
		}
	}
	corrupt := append([]byte(nil), data...)
	corrupt[codecHeaderSize] ^= 0xff
	if _, _, err := Open(bytes.NewReader(corrupt), DefaultCodecLimits()); !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("checksum corruption error = %v", err)
	}
	trailing := append(append([]byte(nil), data...), 0)
	if _, _, err := Open(bytes.NewReader(trailing), DefaultCodecLimits()); !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("trailing data error = %v", err)
	}
	unknownVersion := append([]byte(nil), data...)
	binary.LittleEndian.PutUint16(unknownVersion[4:6], codecVersion+1)
	if _, _, err := Open(bytes.NewReader(unknownVersion), DefaultCodecLimits()); !errors.Is(err, ErrUnsupportedCodec) {
		t.Fatalf("unknown version error = %v", err)
	}
	limits := DefaultCodecLimits()
	limits.MaxVectors = 1
	if _, _, err := Open(bytes.NewReader(data), limits); !errors.Is(err, ErrSegmentLimit) {
		t.Fatalf("vector limit error = %v", err)
	}
}

func TestCodecRejectsNonCanonicalPreparedVector(t *testing.T) {
	idx := newTestIndex(t, 10, 10)
	if _, err := idx.AppendBatch([][]float32{{1, 2}}); err != nil {
		t.Fatal(err)
	}
	data, _, err := Marshal(idx.Freeze())
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint32(data[codecHeaderSize:codecHeaderSize+4], 1<<31)
	rewriteCodecChecksum(data)
	if _, _, err := Open(bytes.NewReader(data), DefaultCodecLimits()); !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("negative-zero error = %v", err)
	}
}

func FuzzOpenCodec(f *testing.F) {
	idx, err := New(Config{Dimensions: 2, Metric: vector.MetricL2Squared, MaxVectors: 2, MaxK: 2})
	if err != nil {
		f.Fatal(err)
	}
	_, _ = idx.AppendBatch([][]float32{{1, 2}})
	valid, _, err := Marshal(idx.Freeze())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte("VFLT"))
	f.Fuzz(func(t *testing.T, data []byte) {
		limits := CodecLimits{MaxDimensions: 32, MaxVectors: 100, MaxVectorBytes: 64 << 10, MaxK: 100}
		_, _, _ = Open(bytes.NewReader(data), limits)
	})
}

func rewriteCodecChecksum(data []byte) {
	body := data[:len(data)-codecFooterSize]
	binary.LittleEndian.PutUint32(data[len(data)-codecFooterSize:], crc32.ChecksumIEEE(body))
}
