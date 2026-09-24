package semanticpersist

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestCodecRoundTripPreparedSource(t *testing.T) {
	calculator, err := vector.NewCalculator(3, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	source, err := vector.NewMemorySource(calculator, [][]float32{{1, 2, 3}, {3, 2, 1}})
	if err != nil {
		t.Fatal(err)
	}
	data, metadata, err := MarshalSource(source, 5)
	if err != nil {
		t.Fatal(err)
	}
	opened, openedMetadata, err := OpenVectorSource(bytes.NewReader(data), DefaultCodecLimits())
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
	source, err := vector.NewMemorySource(calculator, [][]float32{{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	data, _, err := MarshalSource(source, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := OpenVectorSource(bytes.NewReader(data[:len(data)-1]), DefaultCodecLimits()); err == nil {
		t.Fatal("truncated codec accepted")
	}
	corrupt := append([]byte(nil), data...)
	corrupt[codecHeaderSize] ^= 0xff
	if _, _, err := OpenVectorSource(bytes.NewReader(corrupt), DefaultCodecLimits()); !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("corrupt codec error = %v", err)
	}
}
