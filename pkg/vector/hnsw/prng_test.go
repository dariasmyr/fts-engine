package hnsw

import (
	"math"
	"slices"
	"testing"
)

func TestLevelRNGGoldenSplitMix64(t *testing.T) {
	rng := newLevelRNG(0)
	want := []uint64{
		0xe220a8397b1dcdaf,
		0x6e789e6aa1b965f4,
		0x06c45d188009454f,
		0xf88bb8a8724c81ec,
		0x1b39896a51a8749b,
		0x53cb9f0c747ea2ea,
	}
	got := make([]uint64, len(want))
	for i := range got {
		got[i] = rng.nextUint64()
	}
	if !slices.Equal(got, want) {
		t.Fatalf("SplitMix64 sequence = %016x, want %016x", got, want)
	}
}

func TestLevelFromUnitBoundariesAndCap(t *testing.T) {
	if got := levelFromUnit(1, 2); got != 0 {
		t.Fatalf("levelFromUnit(1, 2) = %d, want 0", got)
	}
	if got := levelFromUnit(0.25, 2); got != 2 {
		t.Fatalf("levelFromUnit(0.25, 2) = %d, want 2", got)
	}
	if got := levelFromUnit(math.SmallestNonzeroFloat64, 2); got != MaxLevel {
		t.Fatalf("capped level = %d, want %d", got, MaxLevel)
	}
}

func TestLevelRNGGoldenLevels(t *testing.T) {
	rng := newLevelRNG(0)
	want := []uint8{0, 1, 5, 0, 3, 1, 2, 0, 2, 0, 1, 0}
	got := make([]uint8, len(want))
	for i := range got {
		got[i] = rng.level(2)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("level sequence = %v, want %v", got, want)
	}

	first := newLevelRNG(0xfeedface)
	second := newLevelRNG(0xfeedface)
	for i := 0; i < 128; i++ {
		if got, want := first.level(16), second.level(16); got != want {
			t.Fatalf("same-seed level %d = %d, want %d", i, got, want)
		}
	}
}
