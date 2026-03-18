package hamt

import "testing"

func TestIndexInsertAndSearch(t *testing.T) {
	idx := New()

	if err := idx.Insert("hotel", "doc-1"); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("len(docs) = %d, want 1", len(docs))
	}
	if docs[0].ID != "doc-1" {
		t.Fatalf("doc ID = %q, want %q", docs[0].ID, "doc-1")
	}
}

func TestIndexInsertSameDocIncrementsCount(t *testing.T) {
	idx := New()

	_ = idx.Insert("hotel", "doc-1")
	_ = idx.Insert("hotel", "doc-1")

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("len(docs) = %d, want 1", len(docs))
	}
	if docs[0].Count != 2 {
		t.Fatalf("doc Count = %d, want 2", docs[0].Count)
	}
}

func TestIndexInsertDifferentDocs(t *testing.T) {
	idx := New()

	_ = idx.Insert("hotel", "doc-1")
	_ = idx.Insert("hotel", "doc-2")

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("len(docs) = %d, want 2", len(docs))
	}
}

func TestIndexSearchNotFound(t *testing.T) {
	idx := New()

	docs, err := idx.Search("unknown")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 0 {
		t.Fatalf("len(docs) = %d, want 0", len(docs))
	}
}

func TestIndexAnalyze(t *testing.T) {
	idx := New()
	_ = idx.Insert("hotel", "doc-1")

	stats := idx.Analyze()
	if stats.Nodes == 0 {
		t.Fatalf("stats.Nodes = %d, want > 0", stats.Nodes)
	}
}
