package semanticpersist

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type testPreparedVectorStore struct {
	calculator vector.Calculator
	values     []float32
}

func newTestPreparedVectorStore(
	t *testing.T,
	calculator vector.Calculator,
	rows [][]float32,
) *testPreparedVectorStore {
	t.Helper()

	dimensions := calculator.Dimensions()
	values := make([]float32, len(rows)*dimensions)

	for row, value := range rows {
		start := row * dimensions

		if err := calculator.PrepareInto(
			values[start:start+dimensions],
			value,
		); err != nil {
			t.Fatal(err)
		}
	}

	return &testPreparedVectorStore{
		calculator: calculator,
		values:     values,
	}
}

func (s *testPreparedVectorStore) Len() int {
	return len(s.values) / s.calculator.Dimensions()
}

func (s *testPreparedVectorStore) Dimensions() int {
	return s.calculator.Dimensions()
}

func (s *testPreparedVectorStore) Metric() vector.Metric {
	return s.calculator.Metric()
}

func (s *testPreparedVectorStore) Normalization() vector.Normalization {
	return s.calculator.Normalization()
}

func (s *testPreparedVectorStore) ReadVectorInto(
	ctx context.Context,
	ordinal vector.Ordinal,
	dst []float32,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	start := int(ordinal) * s.Dimensions()
	copy(dst, s.values[start:start+s.Dimensions()])

	return nil
}

var _ vector.PreparedVectorStore = (*testPreparedVectorStore)(nil)

func TestCodecRoundTripPreparedSource(t *testing.T) {
	calculator, err := vector.NewCalculator(3, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}

	source := newTestPreparedVectorStore(
		t,
		calculator,
		[][]float32{
			{1, 2, 3},
			{3, 2, 1},
		},
	)

	var buffer bytes.Buffer

	metadata, err := writeVectorFile(
		context.Background(),
		&buffer,
		source,
		5,
		true,
	)
	if err != nil {
		t.Fatal(err)
	}

	data := buffer.Bytes()

	if string(data[:4]) != "SVEC" {
		t.Fatalf("magic = %q, want %q", data[:4], "SVEC")
	}

	if got := binary.LittleEndian.Uint16(data[4:6]); got != codecVersion {
		t.Fatalf("version = %d, want %d", got, codecVersion)
	}

	opened, openedMetadata, err := openBytes(
		data,
		defaultCodecLimits(),
	)
	if err != nil {
		t.Fatal(err)
	}

	if openedMetadata != metadata {
		t.Fatalf(
			"metadata = %+v, want %+v",
			openedMetadata,
			metadata,
		)
	}

	if opened.Calculator.Dimensions() != source.Dimensions() {
		t.Fatalf(
			"dimensions = %d, want %d",
			opened.Calculator.Dimensions(),
			source.Dimensions(),
		)
	}

	if opened.Calculator.Metric() != source.Metric() {
		t.Fatalf(
			"metric = %v, want %v",
			opened.Calculator.Metric(),
			source.Metric(),
		)
	}

	if opened.Calculator.Normalization() != source.Normalization() {
		t.Fatalf(
			"normalization = %v, want %v",
			opened.Calculator.Normalization(),
			source.Normalization(),
		)
	}

	if !slices.Equal(opened.Values, source.values) {
		t.Fatalf(
			"values = %v, want %v",
			opened.Values,
			source.values,
		)
	}
}

func TestCodecRejectsTruncatedAndCorruptData(t *testing.T) {
	calculator, err := vector.NewCalculator(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}

	source := newTestPreparedVectorStore(
		t,
		calculator,
		[][]float32{{1, 2}},
	)

	var buffer bytes.Buffer

	if _, err := writeVectorFile(context.Background(), &buffer, source, 2, true); err != nil {
		t.Fatal(err)
	}

	data := buffer.Bytes()

	t.Run("truncated", func(t *testing.T) {
		_, _, err := openBytes(
			data[:len(data)-1],
			defaultCodecLimits(),
		)
		if err == nil {
			t.Fatal("truncated codec accepted")
		}
	})

	t.Run("corrupt payload", func(t *testing.T) {
		corrupt := append([]byte(nil), data...)
		corrupt[codecHeaderSize] ^= 0xff

		_, _, err := openBytes(
			corrupt,
			defaultCodecLimits(),
		)
		if !errors.Is(err, errCorruptVectorFile) {
			t.Fatalf(
				"corrupt codec error = %v, want %v",
				err,
				errCorruptVectorFile,
			)
		}
	})

	t.Run("old magic", func(t *testing.T) {
		oldIdentity := append([]byte(nil), data...)
		copy(oldIdentity[:4], "VFLT")

		_, _, err := openBytes(
			oldIdentity,
			defaultCodecLimits(),
		)
		if !errors.Is(err, errCorruptVectorFile) {
			t.Fatalf(
				"old identity error = %v, want %v",
				err,
				errCorruptVectorFile,
			)
		}
	})

	t.Run("old version", func(t *testing.T) {
		oldVersion := append([]byte(nil), data...)
		binary.LittleEndian.PutUint16(oldVersion[4:6], 1)

		_, _, err := openBytes(
			oldVersion,
			defaultCodecLimits(),
		)
		if !errors.Is(err, errUnsupportedVectorFile) {
			t.Fatalf(
				"old version error = %v, want %v",
				err,
				errUnsupportedVectorFile,
			)
		}
	})
}

func TestWriteVectorFileStopsBeforeReading(t *testing.T) {
	calculator, err := vector.NewCalculator(1, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}

	source := newTestPreparedVectorStore(
		t,
		calculator,
		[][]float32{{1}},
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = writeVectorFile(
		ctx,
		&bytes.Buffer{},
		source,
		1,
		true,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"writeVectorFile() = %v, want %v",
			err,
			context.Canceled,
		)
	}
}

func FuzzOpenBytes(f *testing.F) {
	calculator, err := vector.NewCalculator(2, vector.MetricL2Squared)
	if err != nil {
		f.Fatal(err)
	}

	source := &testPreparedVectorStore{
		calculator: calculator,
		values:     []float32{1, 2},
	}

	var buffer bytes.Buffer

	if _, err := writeVectorFile(
		context.Background(),
		&buffer,
		source,
		2,
		true,
	); err != nil {
		f.Fatal(err)
	}

	f.Add(buffer.Bytes())

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = openBytes(
			data,
			codecLimitsConfig{
				MaxDimensions:  16,
				MaxVectors:     32,
				MaxVectorBytes: 4096,
				MaxK:           32,
			},
		)
	})
}
