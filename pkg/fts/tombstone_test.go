package fts

import "testing"

func TestTombstonesEmptyByDefault(t *testing.T) {
	tombs := NewTombstones()

	if tombs.Any() {
		t.Fatal("Any() = true, want false")
	}
	if tombs.IsSet(0) {
		t.Fatal("IsSet(0) = true, want false")
	}
	if got := tombs.Count(); got != 0 {
		t.Fatalf("Count() = %d, want 0", got)
	}
}

func TestTombstonesSetMarksOrd(t *testing.T) {
	tombs := NewTombstones()
	tombs.Set(3)
	tombs.Set(130)

	if !tombs.Any() {
		t.Fatal("Any() = false, want true")
	}
	if !tombs.IsSet(3) {
		t.Fatal("IsSet(3) = false, want true")
	}
	if !tombs.IsSet(130) {
		t.Fatal("IsSet(130) = false, want true")
	}
	if tombs.IsSet(4) {
		t.Fatal("IsSet(4) = true, want false")
	}
	if got := tombs.Count(); got != 2 {
		t.Fatalf("Count() = %d, want 2", got)
	}
}

func TestTombstonesDuplicateSetDoesNotChangeCount(t *testing.T) {
	tombs := NewTombstones()
	tombs.Set(7)
	tombs.Set(7)

	if got := tombs.Count(); got != 1 {
		t.Fatalf("Count() = %d, want 1", got)
	}
}

func TestTombstonesSnapshotRoundTrip(t *testing.T) {
	original := NewTombstones()
	original.Set(1)
	original.Set(128)

	restored := RestoreTombstones(original.Snapshot())

	if !restored.IsSet(1) {
		t.Fatal("restored IsSet(1) = false, want true")
	}
	if !restored.IsSet(128) {
		t.Fatal("restored IsSet(128) = false, want true")
	}
	if restored.IsSet(2) {
		t.Fatal("restored IsSet(2) = true, want false")
	}
	if got := restored.Count(); got != 2 {
		t.Fatalf("restored Count() = %d, want 2", got)
	}
}

func TestTombstonesZeroValueWorks(t *testing.T) {
	var tombs Tombstones
	tombs.Set(64)

	if !tombs.Any() {
		t.Fatal("Any() = false, want true")
	}
	if !tombs.IsSet(64) {
		t.Fatal("IsSet(64) = false, want true")
	}
	if got := tombs.Count(); got != 1 {
		t.Fatalf("Count() = %d, want 1", got)
	}
}

func TestTombstonesLoadFactor(t *testing.T) {
	tombs := NewTombstones()
	tombs.Set(1)
	tombs.Set(3)

	if got, want := tombs.LoadFactor(4), 0.5; got != want {
		t.Fatalf("LoadFactor(4) = %v, want %v", got, want)
	}
	if got := tombs.LoadFactor(0); got != 0 {
		t.Fatalf("LoadFactor(0) = %v, want 0", got)
	}
}
