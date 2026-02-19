package filters

import "testing"

func TestBloomFilterNoFalseNegatives(t *testing.T) {
	f := NewBloomFilter(BloomConfig{
		Capacity: 1024,
		Hashes:   5,
	})

	keys := []string{"hotel", "wikipedia", "search", "engine", "radix"}
	for _, k := range keys {
		f.Add(k)
	}

	for _, k := range keys {
		if !f.MightContain(k) {
			t.Fatalf("expected key %q to be present", k)
		}
	}
}

func TestFilterFactoryVariantsNoFalseNegatives(t *testing.T) {
	filters := []Filter{
		NewNoopFilter(),
		NewBloomFilter(BloomConfig{Capacity: 1024, Hashes: 5}),
		NewCuckooFilter(CuckooConfig{Capacity: 1024, BucketSz: 4, MaxKicks: 100}),
		NewRibbonFilter(RibbonConfig{Bits: 4096, Width: 8}),
	}

	keys := []string{"hotel", "wikipedia", "search", "engine", "radix"}
	for i, f := range filters {
		for _, k := range keys {
			f.Add(k)
		}
		for _, k := range keys {
			if !f.MightContain(k) {
				t.Fatalf("filter[%d]: expected key %q to be present", i, k)
			}
		}
	}
}
