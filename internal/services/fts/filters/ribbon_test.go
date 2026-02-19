package filters

import "testing"

func TestRibbonFilterNoFalseNegatives(t *testing.T) {
	f := NewRibbonFilter(RibbonConfig{
		Bits:  2048,
		Width: 8,
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
