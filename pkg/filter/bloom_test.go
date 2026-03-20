package filter

import (
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"testing"
)

func TestBloomFilterAddAndContains(t *testing.T) {
	bf := NewBloomFilter(100, 10, 3)

	if bf.Contains([]byte("alpha")) {
		t.Fatalf("Contains(alpha) before Add = true, want false")
	}

	bf.Add([]byte("alpha"))

	if !bf.Contains([]byte("alpha")) {
		t.Fatalf("Contains(alpha) after Add = false, want true")
	}
}

func TestRegistryRegisterAndNew(t *testing.T) {
	name := "test-bloom-registry"

	err := fts.RegisterFilter(name, func() (Filter, error) {
		return NewBloomFilter(100, 10, 3), nil
	})
	if err != nil {
		t.Fatalf("RegisterFilter() error = %v", err)
	}

	f, err := fts.NewFilter(name)
	if err != nil {
		t.Fatalf("NewFilter() error = %v", err)
	}

	f.Add([]byte("beta"))
	if !f.Contains([]byte("beta")) {
		t.Fatalf("Contains(beta) = false, want true")
	}
}
