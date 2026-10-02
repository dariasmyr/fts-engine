package semanticpersist

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

func TestCodecRoundTripPreparedSource(t *testing.T) {
	calculator, err := vector.NewCalculator(3, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	source, err := vectorstore.NewMemoryVectorStore(calculator, [][]float32{{1, 2, 3}, {3, 2, 1}})
	if err != nil {
		t.Fatal(err)
	}
	var buffer bytes.Buffer
	metadata, err := WriteVectorFile(context.Background(), &buffer, source, 5)
	if err != nil {
		t.Fatal(err)
	}
	data := buffer.Bytes()
	opened, openedMetadata, err := OpenVectorFile(bytes.NewReader(data), DefaultCodecLimits())
	if err != nil {
		t.Fatal(err)
	}
	if openedMetadata != metadata || opened.Len() != source.Len() || opened.Dimensions() != source.Dimensions() || opened.Metric() != source.Metric() {
		t.Fatalf("opened metadata/source mismatch: %+v %+v", openedMetadata, opened)
	}
	for ordinal := range source.Len() {
		want := make([]float32, source.Dimensions())
		got := make([]float32, opened.Dimensions())
		if err := source.ReadVectorInto(context.Background(), vector.Ordinal(ordinal), want); err != nil {
			t.Fatal(err)
		}
		if err := opened.ReadVectorInto(context.Background(), vector.Ordinal(ordinal), got); err != nil {
			t.Fatal(err)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("row %d = %v, want %v", ordinal, got, want)
			}
		}
	}
}

func TestCodecRejectsTruncatedAndCorruptData(t *testing.T) {
	calculator, err := vector.NewCalculator(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	source, err := vectorstore.NewMemoryVectorStore(calculator, [][]float32{{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	var buffer bytes.Buffer
	_, err = WriteVectorFile(context.Background(), &buffer, source, 2)
	if err != nil {
		t.Fatal(err)
	}
	data := buffer.Bytes()
	if _, _, err := OpenVectorFile(bytes.NewReader(data[:len(data)-1]), DefaultCodecLimits()); err == nil {
		t.Fatal("truncated codec accepted")
	}
	corrupt := append([]byte(nil), data...)
	corrupt[codecHeaderSize] ^= 0xff
	if _, _, err := OpenVectorFile(bytes.NewReader(corrupt), DefaultCodecLimits()); !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("corrupt codec error = %v", err)
	}
}

func TestWriteVectorFileStopsBeforeReading(t *testing.T) {
	calculator, err := vector.NewCalculator(1, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	source, err := vectorstore.NewMemoryVectorStore(calculator, [][]float32{{1}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = WriteVectorFile(ctx, &bytes.Buffer{}, source, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteVectorFile() = %v, want context.Canceled", err)
	}
}

func FuzzOpenVectorFile(f *testing.F) {
	calculator, err := vector.NewCalculator(2, vector.MetricL2Squared)
	if err != nil {
		f.Fatal(err)
	}
	source, err := vectorstore.NewMemoryVectorStore(calculator, [][]float32{{1, 2}})
	if err != nil {
		f.Fatal(err)
	}
	var buffer bytes.Buffer
	if _, err := WriteVectorFile(context.Background(), &buffer, source, 2); err != nil {
		f.Fatal(err)
	}
	f.Add(buffer.Bytes())
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = OpenVectorFile(bytes.NewReader(data), CodecLimits{MaxDimensions: 16, MaxVectors: 32, MaxVectorBytes: 4096, MaxK: 32})
	})
}
