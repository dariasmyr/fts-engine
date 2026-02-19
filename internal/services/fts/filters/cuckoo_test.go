package filters

import "testing"

func TestCuckooFilterNoFalseNegatives(t *testing.T) {
	f := NewCuckooFilter(CuckooConfig{
		Capacity: 1024,
		BucketSz: 4,
		MaxKicks: 200,
	})

	keys := []string{"hotel", "wikipedia", "search", "engine", "radix", "hamt", "trigram"}
	for _, k := range keys {
		f.Add(k)
	}

	for _, k := range keys {
		if !f.MightContain(k) {
			t.Fatalf("expected key %q to be present", k)
		}
	}
}
