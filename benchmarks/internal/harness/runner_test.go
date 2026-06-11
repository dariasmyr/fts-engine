package harness

import (
	"testing"
	"time"
)

func TestPercentiles(t *testing.T) {
	lats := []time.Duration{4 * time.Millisecond, 1 * time.Millisecond, 3 * time.Millisecond, 2 * time.Millisecond}
	got := Percentiles(lats, 0, 0.50, 0.95, 1)
	want := []time.Duration{1 * time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Percentiles()[%d] = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestPercentilesEmpty(t *testing.T) {
	got := Percentiles(nil, 0.5, 0.95)
	if len(got) != 2 {
		t.Fatalf("len(Percentiles(nil)) = %d, want 2", len(got))
	}
	for i, v := range got {
		if v != 0 {
			t.Fatalf("Percentiles(nil)[%d] = %s, want 0", i, v)
		}
	}
}
