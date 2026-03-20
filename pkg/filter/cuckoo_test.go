package filter

import (
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func TestCuckooFilterInsertAndContains(t *testing.T) {
	cf := NewCuckooFilter(256, 4, 200)

	if cf.Contains([]byte("alpha")) {
		t.Fatalf("Contains(alpha) before Insert = true, want false")
	}

	ok := cf.Add([]byte("alpha"))
	if !ok {
		t.Fatalf("Insert(alpha) = false, want true")
	}

	if !cf.Contains([]byte("alpha")) {
		t.Fatalf("Contains(alpha) after Insert = false, want true")
	}
}

func TestCuckooAsFilter(t *testing.T) {
	var f fts.Filter = NewCuckooFilter(256, 4, 200)

	f.Add([]byte("beta"))

	if !f.Contains([]byte("beta")) {
		t.Fatalf("Contains(beta) = false, want true")
	}
}
