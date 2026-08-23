package vector

import (
	"errors"
	"math/rand"
	"testing"
)

func TestBitSetContainsAndCardinality(t *testing.T) {
	set, err := NewBitSet(130, 0, 64, 129, 64)
	if err != nil {
		t.Fatalf("NewBitSet() error = %v", err)
	}
	if set.Size() != 130 {
		t.Fatalf("Size() = %d, want 130", set.Size())
	}
	if set.Cardinality() != 3 {
		t.Fatalf("Cardinality() = %d, want 3", set.Cardinality())
	}
	for _, ord := range []Ordinal{0, 64, 129} {
		if !set.Contains(ord) {
			t.Fatalf("Contains(%d) = false, want true", ord)
		}
	}
	if set.Contains(63) || set.Contains(130) {
		t.Fatal("Contains() accepted an unset or out-of-range ordinal")
	}
}

func TestNewBitSetRejectsOutOfRangeOrdinal(t *testing.T) {
	if _, err := NewBitSet(3, 3); !errors.Is(err, ErrOrdinalOutOfRange) {
		t.Fatalf("NewBitSet(3, 3) error = %v, want ErrOrdinalOutOfRange", err)
	}
}

func TestFullBitSetMasksTrailingBits(t *testing.T) {
	set := NewFullBitSet(65)
	if set.Cardinality() != 65 {
		t.Fatalf("Cardinality() = %d, want 65", set.Cardinality())
	}
	for ord := range 65 {
		if !set.Contains(Ordinal(ord)) {
			t.Fatalf("Contains(%d) = false, want true", ord)
		}
	}
	if set.Contains(65) {
		t.Fatal("Contains(65) = true, want false")
	}
}

func TestBitSetWithDoesNotMutateOldSnapshot(t *testing.T) {
	original, err := NewBitSet(8, 1, 2)
	if err != nil {
		t.Fatalf("NewBitSet() error = %v", err)
	}
	updated, err := original.With(1, false)
	if err != nil {
		t.Fatalf("With(1, false) error = %v", err)
	}
	updated, err = updated.With(7, true)
	if err != nil {
		t.Fatalf("With(7, true) error = %v", err)
	}

	if !original.Contains(1) || original.Contains(7) || original.Cardinality() != 2 {
		t.Fatalf("original snapshot mutated: cardinality=%d", original.Cardinality())
	}
	if updated.Contains(1) || !updated.Contains(2) || !updated.Contains(7) || updated.Cardinality() != 2 {
		t.Fatalf("updated snapshot = unexpected state, cardinality=%d", updated.Cardinality())
	}
}

func TestBitSetWithRejectsOutOfRangeOrdinal(t *testing.T) {
	set := NewFullBitSet(2)
	if _, err := set.With(2, false); !errors.Is(err, ErrOrdinalOutOfRange) {
		t.Fatalf("With(2, false) error = %v, want ErrOrdinalOutOfRange", err)
	}
}

func TestEmptyBitSet(t *testing.T) {
	set, err := NewBitSet(0)
	if err != nil {
		t.Fatalf("NewBitSet(0) error = %v", err)
	}
	if set.Size() != 0 || set.Cardinality() != 0 || set.Contains(0) {
		t.Fatalf("empty BitSet = size %d cardinality %d", set.Size(), set.Cardinality())
	}

	full := NewFullBitSet(0)
	if full.Size() != 0 || full.Cardinality() != 0 || full.Contains(0) {
		t.Fatalf("empty full BitSet = size %d cardinality %d", full.Size(), full.Cardinality())
	}
}

func TestBitSetMatchesBooleanModel(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for _, size := range []uint32{1, 63, 64, 65, 127, 128, 129} {
		set, err := NewBitSet(size)
		if err != nil {
			t.Fatalf("size %d NewBitSet() error = %v", size, err)
		}
		model := make([]bool, size)
		for step := range 500 {
			ord := Ordinal(rng.Intn(int(size)))
			accepted := rng.Intn(2) == 0
			previous := set
			previousValue := previous.Contains(ord)
			set, err = set.With(ord, accepted)
			if err != nil {
				t.Fatalf("size %d step %d With() error = %v", size, step, err)
			}
			if previous.Contains(ord) != previousValue {
				t.Fatalf("size %d step %d previous snapshot mutated", size, step)
			}
			model[ord] = accepted

			wantCardinality := 0
			for i, want := range model {
				if want {
					wantCardinality++
				}
				if got := set.Contains(Ordinal(i)); got != want {
					t.Fatalf("size %d step %d Contains(%d) = %t, want %t", size, step, i, got, want)
				}
			}
			if set.Cardinality() != wantCardinality {
				t.Fatalf("size %d step %d Cardinality() = %d, want %d", size, step, set.Cardinality(), wantCardinality)
			}
		}
	}
}

func TestBitSetWithChangesGrowsAndAppliesBatch(t *testing.T) {
	set, err := NewBitSet(3, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	next, err := set.WithChanges(6, []Ordinal{3, 5}, []Ordinal{0})
	if err != nil {
		t.Fatal(err)
	}
	if set.Size() != 3 || set.Cardinality() != 2 || !set.Contains(0) {
		t.Fatalf("original set changed: size=%d cardinality=%d", set.Size(), set.Cardinality())
	}
	if next.Size() != 6 || next.Cardinality() != 3 {
		t.Fatalf("next = size %d cardinality %d, want 6 and 3", next.Size(), next.Cardinality())
	}
	for _, ord := range []Ordinal{2, 3, 5} {
		if !next.Contains(ord) {
			t.Fatalf("ordinal %d is rejected, want accepted", ord)
		}
	}
	if next.Contains(0) || next.Contains(1) || next.Contains(4) {
		t.Fatal("unexpected accepted ordinal")
	}
}

func TestBitSetWithChangesRejectsInvalidChanges(t *testing.T) {
	set := NewFullBitSet(2)
	if _, err := set.WithChanges(1, nil, nil); !errors.Is(err, ErrBitSetShrink) {
		t.Fatalf("shrink error = %v, want ErrBitSetShrink", err)
	}
	if _, err := set.WithChanges(3, []Ordinal{2}, []Ordinal{2}); !errors.Is(err, ErrConflictingBits) {
		t.Fatalf("conflict error = %v, want ErrConflictingBits", err)
	}
}

func TestBitSetWithChangesAcrossBlocksKeepsOldSnapshot(t *testing.T) {
	set := NewFullBitSet(10_000)
	next, err := set.WithChanges(12_000, []Ordinal{11_999}, []Ordinal{1, 9_000})
	if err != nil {
		t.Fatal(err)
	}
	if !set.Contains(1) || !set.Contains(9_000) || set.Size() != 10_000 {
		t.Fatal("old block snapshot changed")
	}
	if next.Contains(1) || next.Contains(9_000) || !next.Contains(11_999) {
		t.Fatal("cross-block changes were not applied")
	}
	if next.Cardinality() != 9_999 {
		t.Fatalf("cardinality = %d, want 9999", next.Cardinality())
	}
}
