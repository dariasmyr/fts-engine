package semantic

import (
	"context"
	"errors"
	"testing"
)

func TestLocationTableWithChangesPreservesBase(t *testing.T) {
	t.Parallel()

	base := newLocationTable()
	if err := base.set(1, vectorLocation{segment: 1, ordinal: 0}); err != nil {
		t.Fatal(err)
	}
	if err := base.set(2, vectorLocation{segment: 1, ordinal: 1}); err != nil {
		t.Fatal(err)
	}
	removed := VectorID(1)
	added := VectorID(3)
	next, err := base.withChanges(t.Context(), []VectorID{removed}, map[VectorID]vectorLocation{
		added: {segment: 2, ordinal: 7},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := base.get(removed); !ok {
		t.Fatal("base table was mutated by removal")
	}
	if _, ok := base.get(added); ok {
		t.Fatal("base table was mutated by addition")
	}
	if _, ok := next.get(removed); ok {
		t.Fatal("removed location remains in candidate")
	}
	location, ok := next.get(added)
	if !ok || location != (vectorLocation{segment: 2, ordinal: 7}) {
		t.Fatalf("added location = %+v, %v", location, ok)
	}
	if len(base) != 2 || len(next) != len(base) {
		t.Fatalf("location counts: base=%d next=%d", len(base), len(next))
	}
	if location, ok := next.get(2); !ok || location != (vectorLocation{segment: 1, ordinal: 1}) {
		t.Fatalf("unchanged location = %+v, %v", location, ok)
	}
}

func TestLocationTableWithChangesHonorsCancellation(t *testing.T) {
	t.Parallel()

	base := newLocationTable()
	if err := base.set(1, vectorLocation{segment: 1}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := base.withChanges(ctx, []VectorID{1}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("withChanges() error = %v, want context.Canceled", err)
	}
}

func TestLocationTableRejectsDuplicateAndMissingChanges(t *testing.T) {
	t.Parallel()

	base := newLocationTable()
	if err := base.set(1, vectorLocation{segment: 1}); err != nil {
		t.Fatal(err)
	}
	if err := base.set(1, vectorLocation{segment: 2}); !errors.Is(err, ErrInternalState) {
		t.Fatalf("set(duplicate) error = %v, want ErrInternalState", err)
	}
	if _, err := base.withChanges(t.Context(), []VectorID{2}, nil); !errors.Is(err, ErrInternalState) {
		t.Fatalf("withChanges(missing removal) error = %v, want ErrInternalState", err)
	}
	if _, err := base.withChanges(t.Context(), []VectorID{1, 1}, nil); !errors.Is(err, ErrInternalState) {
		t.Fatalf("withChanges(duplicate removal) error = %v, want ErrInternalState", err)
	}
	if _, err := base.withChanges(t.Context(), nil, map[VectorID]vectorLocation{
		1: {segment: 2},
	}); !errors.Is(err, ErrInternalState) {
		t.Fatalf("withChanges(duplicate addition) error = %v, want ErrInternalState", err)
	}
}
