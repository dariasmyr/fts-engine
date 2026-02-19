package testsuite

import (
	"fts-hw/internal/services/fts/factory"
	"fts-hw/internal/services/fts/filters"
	"testing"
)

func TestFilterContractNoFalseNegatives(t *testing.T) {
	filterOpts := []factory.FilterOptions{
		{Type: "none"},
		{
			Type:  "bloom",
			Bloom: filters.BloomConfig{Capacity: 1024, Hashes: 5},
		},
		{
			Type:   "cuckoo",
			Cuckoo: filters.CuckooConfig{Capacity: 1024, BucketSz: 4, MaxKicks: 100},
		},
		{
			Type:   "ribbon",
			Ribbon: filters.RibbonConfig{Bits: 4096, Width: 8},
		},
	}

	keys := []string{"hotel", "wikipedia", "search", "engine", "radix", "hamt", "trigram"}

	for _, opts := range filterOpts {
		t.Run(opts.Type, func(t *testing.T) {
			filter, err := factory.NewFilter(opts)
			if err != nil {
				t.Fatalf("new filter failed: %v", err)
			}

			for _, key := range keys {
				filter.Add(key)
			}

			for _, key := range keys {
				if !filter.MightContain(key) {
					t.Fatalf("expected key %q to be present", key)
				}
			}
		})
	}
}
