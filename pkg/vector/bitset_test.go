package vector

import (
	"errors"
	"testing"
)

func TestBitSetUpdatesAreImmutable(t *testing.T) {
	original, err := NewBitSet(130, 1, 65)
	if err != nil {
		t.Fatal(err)
	}
	updated, err := original.WithChanges(131, []Ordinal{129, 130}, []Ordinal{1})
	if err != nil {
		t.Fatal(err)
	}
	if !original.Allows(1) || original.Allows(129) || original.TotalOrdinalCount() != 130 {
		t.Fatalf("original mutated: count=%d words=%v", original.TotalOrdinalCount(), original.SnapshotWords())
	}
	if updated.Allows(1) || !updated.Allows(65) || !updated.Allows(129) || !updated.Allows(130) {
		t.Fatalf("updated set has unexpected membership: %v", updated.SnapshotWords())
	}
	if updated.AllowedOrdinalCount() != 3 {
		t.Fatalf("allowed count = %d, want 3", updated.AllowedOrdinalCount())
	}
}

func TestBitSetRejectsInvalidChanges(t *testing.T) {
	set := NewFullBitSet(2)
	if _, err := set.WithChanges(1, nil, nil); !errors.Is(err, ErrBitSetShrink) {
		t.Fatalf("WithChanges(shrink) error = %v, want ErrBitSetShrink", err)
	}
	if _, err := set.WithChanges(2, []Ordinal{1}, []Ordinal{1}); !errors.Is(err, ErrConflictingOrdinals) {
		t.Fatalf("WithChanges(conflict) error = %v, want ErrConflictingOrdinals", err)
	}
}
