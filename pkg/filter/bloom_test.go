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

func TestBloomFilterHashFunctionsUseDifferentSeeds(t *testing.T) {
	bf := NewBloomFilter(100, 10, 4)
	key := []byte("same-key")

	h0 := bf.hashFuncs[0](key)
	allEqual := true
	for i := 1; i < len(bf.hashFuncs); i++ {
		if bf.hashFuncs[i](key) != h0 {
			allEqual = false
			break
		}
	}

	if allEqual {
		t.Fatal("all bloom hash functions returned the same hash; expected different seeded hashes")
	}
}
