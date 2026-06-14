package fts

import "testing"

func TestDocRegistryGetOrAssignStableOrd(t *testing.T) {
	r := NewDocRegistry()

	a1 := r.GetOrAssign("doc-a")
	a2 := r.GetOrAssign("doc-a")
	b := r.GetOrAssign("doc-b")

	if a1 != a2 {
		t.Fatalf("same id should reuse ord: %d != %d", a1, a2)
	}
	if a1 == b {
		t.Fatalf("different ids should get different ords: both %d", a1)
	}
	if got := r.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
}

func TestDocRegistryLookupAndHas(t *testing.T) {
	r := NewDocRegistry()
	ord := r.GetOrAssign("doc-1")

	if got := r.Lookup(ord); got != "doc-1" {
		t.Fatalf("Lookup(%d) = %q, want doc-1", ord, got)
	}
	if got := r.Lookup(ord + 1); got != "" {
		t.Fatalf("Lookup(missing) = %q, want empty", got)
	}
	if gotOrd, ok := r.Has("doc-1"); !ok || gotOrd != ord {
		t.Fatalf("Has(doc-1) = (%d, %v), want (%d, true)", gotOrd, ok, ord)
	}
	if _, ok := r.Has("missing"); ok {
		t.Fatal("Has(missing) = true, want false")
	}
}

func TestDocRegistryForgetKeepsOrdSpaceStable(t *testing.T) {
	r := NewDocRegistry()
	a := r.GetOrAssign("doc-a")
	b := r.GetOrAssign("doc-b")
	r.Forget("doc-a")

	if _, ok := r.Has("doc-a"); ok {
		t.Fatal("doc-a should be forgotten")
	}
	if got := r.Lookup(a); got != "doc-a" {
		t.Fatalf("Lookup(old ord) = %q, want doc-a", got)
	}
	if got := r.Lookup(b); got != "doc-b" {
		t.Fatalf("Lookup(doc-b ord) = %q, want doc-b", got)
	}
	newOrd := r.GetOrAssign("doc-a")
	if newOrd == a {
		t.Fatalf("reassigned ord = %d, want new ord after Forget", newOrd)
	}
	if got := r.Len(); got != 3 {
		t.Fatalf("Len() after reassign = %d, want 3", got)
	}
}

func TestDocRegistrySnapshotRoundTrip(t *testing.T) {
	original := NewDocRegistry()
	ordA := original.GetOrAssign("doc-a")
	ordB := original.GetOrAssign("doc-b")

	restored := RestoreDocRegistry(original.Snapshot())

	if got := restored.Lookup(ordA); got != "doc-a" {
		t.Fatalf("restored Lookup(ordA) = %q, want doc-a", got)
	}
	if got := restored.Lookup(ordB); got != "doc-b" {
		t.Fatalf("restored Lookup(ordB) = %q, want doc-b", got)
	}
	if gotOrd, ok := restored.Has("doc-b"); !ok || gotOrd != ordB {
		t.Fatalf("restored Has(doc-b) = (%d, %v), want (%d, true)", gotOrd, ok, ordB)
	}
}

func TestDocRegistryZeroValueWorks(t *testing.T) {
	var r DocRegistry
	ord := r.GetOrAssign("doc-1")

	if ord != 0 {
		t.Fatalf("first ord = %d, want 0", ord)
	}
	if got := r.Lookup(ord); got != "doc-1" {
		t.Fatalf("Lookup(%d) = %q, want doc-1", ord, got)
	}
}
