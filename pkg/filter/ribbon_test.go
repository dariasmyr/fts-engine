package filter

import (
	"bytes"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func TestRibbonFilterBuildAndContains(t *testing.T) {
	items := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}

	rf, err := NewRibbonFilterWithRetries(32, 32, 24, 7, items, 10)
	if err != nil {
		t.Fatalf("NewRibbonFilterWithRetries() error = %v", err)
	}

	for _, item := range items {
		if !rf.Contains(item) {
			t.Fatalf("Contains(%q) = false, want true", string(item))
		}
	}
}

func TestRibbonFilterWindowValidation(t *testing.T) {
	_, err := NewRibbonFilter(10, 10, 33, 1)
	if err == nil {
		t.Fatal("NewRibbonFilter() error = nil, want non-nil for w=33")
	}
}

func TestRibbonFilterSerializeLoadRoundTrip(t *testing.T) {
	items := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}

	rf, err := NewRibbonFilterWithRetries(32, 32, 24, 7, items, 10)
	if err != nil {
		t.Fatalf("NewRibbonFilterWithRetries() error = %v", err)
	}

	var payload bytes.Buffer
	if err := rf.Serialize(&payload); err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	loaded, err := LoadRibbonFilter(bytes.NewReader(payload.Bytes()))
	if err != nil {
		t.Fatalf("LoadRibbonFilter() error = %v", err)
	}

	for _, item := range items {
		if !loaded.Contains(item) {
			t.Fatalf("loaded.Contains(%q) = false, want true", string(item))
		}
	}
}

func TestRibbonFilterAsFTSFilterViaLazyAdapter(t *testing.T) {
	rf, err := NewRibbonFilter(64, 64, 24, 11)
	if err != nil {
		t.Fatalf("NewRibbonFilter() error = %v", err)
	}

	var f fts.Filter = fts.NewBufferedStaticFilter(rf)
	f.Add([]byte("delta"))

	if !f.Contains([]byte("delta")) {
		t.Fatal("Contains(delta) = false, want true")
	}
}
