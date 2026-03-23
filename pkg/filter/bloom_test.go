package filter

import (
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

func TestBloomFilterFactoryEquivalent(t *testing.T) {
	f := NewBloomFilter(100, 10, 3)

	f.Add([]byte("beta"))
	if !f.Contains([]byte("beta")) {
		t.Fatalf("Contains(beta) = false, want true")
	}
}
