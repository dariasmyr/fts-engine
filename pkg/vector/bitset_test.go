package vector

import (
	"errors"
	"math/rand"
	"testing"
)

func TestBitSetContainsAndCount(t *testing.T) {
	set, err := NewBitSet(130, 0, 64, 129, 64)
	if err != nil {
		t.Fatalf("NewBitSet() error = %v", err)
	}
	if set.TotalOrdinalCount() != 130 {
		t.Fatalf("TotalOrdinalCount() = %d, want 130", set.TotalOrdinalCount())
	}
	if set.AllowedOrdinalCount() != 3 {
		t.Fatalf("AllowedOrdinalCount() = %d, want 3", set.AllowedOrdinalCount())
	}
	for _, ord := range []Ordinal{0, 64, 129} {
		if !set.Allows(ord) {
			t.Fatalf("Contains(%d) = false, want true", ord)
		}
	}
	if set.Allows(63) || set.Allows(130) {
		t.Fatal("Allows() returned true for an unset or out-of-range ordinal")
	}
}

func TestNewBitSetRejectsOutOfRangeOrdinal(t *testing.T) {
	if _, err := NewBitSet(3, 3); !errors.Is(err, ErrOrdinalOutOfRange) {
		t.Fatalf("NewBitSet(3, 3) error = %v, want ErrOrdinalOutOfRange", err)
	}
}

func TestFullBitSetMasksTrailingBits(t *testing.T) {
	set := NewFullBitSet(65)
	if set.AllowedOrdinalCount() != 65 {
		t.Fatalf("AllowedOrdinalCount() = %d, want 65", set.AllowedOrdinalCount())
	}
	for ord := range 65 {
		if !set.Allows(Ordinal(ord)) {
			t.Fatalf("Contains(%d) = false, want true", ord)
		}
	}
	if set.Allows(65) {
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

	if !original.Allows(1) || original.Allows(7) || original.AllowedOrdinalCount() != 2 {
		t.Fatalf("original snapshot changed: allowed count=%d", original.AllowedOrdinalCount())
	}
	if updated.Allows(1) || !updated.Allows(2) || !updated.Allows(7) || updated.AllowedOrdinalCount() != 2 {
		t.Fatalf("updated snapshot has unexpected state: allowed count=%d", updated.AllowedOrdinalCount())
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
	if set.TotalOrdinalCount() != 0 || set.AllowedOrdinalCount() != 0 || set.Allows(0) {
		t.Fatalf("empty BitSet: total count %d, allowed count %d", set.TotalOrdinalCount(), set.AllowedOrdinalCount())
	}

	full := NewFullBitSet(0)
	if full.TotalOrdinalCount() != 0 || full.AllowedOrdinalCount() != 0 || full.Allows(0) {
		t.Fatalf("empty full BitSet: total count %d, allowed count %d", full.TotalOrdinalCount(), full.AllowedOrdinalCount())
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
			allowed := rng.Intn(2) == 0
			previous := set
			previousValue := previous.Allows(ord)
			set, err = set.With(ord, allowed)
			if err != nil {
				t.Fatalf("size %d step %d With() error = %v", size, step, err)
			}
			if previous.Allows(ord) != previousValue {
				t.Fatalf("size %d step %d previous snapshot mutated", size, step)
			}
			model[ord] = allowed

			wantCount := 0
			for i, want := range model {
				if want {
					wantCount++
				}
				if got := set.Allows(Ordinal(i)); got != want {
					t.Fatalf("size %d step %d Contains(%d) = %t, want %t", size, step, i, got, want)
				}
			}
			if set.AllowedOrdinalCount() != wantCount {
				t.Fatalf("size %d step %d AllowedOrdinalCount() = %d, want %d", size, step, set.AllowedOrdinalCount(), wantCount)
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
	if set.TotalOrdinalCount() != 3 || set.AllowedOrdinalCount() != 2 || !set.Allows(0) {
		t.Fatalf("original set changed: total count=%d allowed count=%d", set.TotalOrdinalCount(), set.AllowedOrdinalCount())
	}
	if next.TotalOrdinalCount() != 6 || next.AllowedOrdinalCount() != 3 {
		t.Fatalf("next = total count %d allowed count %d, want 6 and 3", next.TotalOrdinalCount(), next.AllowedOrdinalCount())
	}
	for _, ord := range []Ordinal{2, 3, 5} {
		if !next.Allows(ord) {
			t.Fatalf("ordinal %d is disallowed, want allowed", ord)
		}
	}
	if next.Allows(0) || next.Allows(1) || next.Allows(4) {
		t.Fatal("unexpected allowed ordinal")
	}
}

func TestBitSetWithChangesRejectsInvalidChanges(t *testing.T) {
	set := NewFullBitSet(2)
	if _, err := set.WithChanges(1, nil, nil); !errors.Is(err, ErrBitSetShrink) {
		t.Fatalf("shrink error = %v, want ErrBitSetShrink", err)
	}
	if _, err := set.WithChanges(3, []Ordinal{2}, []Ordinal{2}); !errors.Is(err, ErrConflictingOrdinals) {
		t.Fatalf("conflict error = %v, want ErrConflictingOrdinals", err)
	}
}

func TestBitSetWithChangesAcrossBlocksKeepsOldSnapshot(t *testing.T) {
	set := NewFullBitSet(10_000)
	next, err := set.WithChanges(12_000, []Ordinal{11_999}, []Ordinal{1, 9_000})
	if err != nil {
		t.Fatal(err)
	}
	if !set.Allows(1) || !set.Allows(9_000) || set.TotalOrdinalCount() != 10_000 {
		t.Fatal("old block snapshot changed")
	}
	if next.Allows(1) || next.Allows(9_000) || !next.Allows(11_999) {
		t.Fatal("cross-block changes were not applied")
	}
	if next.AllowedOrdinalCount() != 9_999 {
		t.Fatalf("allowed count = %d, want 9999", next.AllowedOrdinalCount())
	}
}

func TestBitSetWordSnapshotRoundTrip(t *testing.T) {
	set, err := NewBitSet(130, 0, 64, 129)
	if err != nil {
		t.Fatal(err)
	}
	words := set.SnapshotWords()
	restored, err := NewBitSetFromWords(130, words)
	if err != nil {
		t.Fatal(err)
	}
	if restored.AllowedOrdinalCount() != 3 || !restored.Allows(0) || !restored.Allows(64) || !restored.Allows(129) {
		t.Fatalf("restored set is invalid: allowed count=%d", restored.AllowedOrdinalCount())
	}
	words[0] = 0
	if !restored.Allows(0) {
		t.Fatal("changing input words changed the restored set")
	}
	if _, err := NewBitSetFromWords(65, []uint64{0, 2}); err == nil {
		t.Fatal("non-zero trailing bits were allowed")
	}
}
