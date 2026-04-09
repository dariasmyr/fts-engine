package filter

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func TestRibbonFilterBuildAndContains(t *testing.T) {
	items := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}

	rf, err := NewRibbonFilter(32, 32, 24, 7)
	if err != nil {
		t.Fatalf("NewRibbonFilter() error = %v", err)
	}

	stream := func(emit func([]byte) bool) error {
		for _, item := range items {
			if !emit(item) {
				break
			}
		}
		return nil
	}

	if err := rf.BuildWithRetriesFromKeyStream(stream, 10); err != nil {
		t.Fatalf("BuildWithRetriesFromKeyStream() error = %v", err)
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

	rf, err := NewRibbonFilter(32, 32, 24, 7)
	if err != nil {
		t.Fatalf("NewRibbonFilter() error = %v", err)
	}

	stream := func(emit func([]byte) bool) error {
		for _, item := range items {
			if !emit(item) {
				break
			}
		}
		return nil
	}

	if err := rf.BuildWithRetriesFromKeyStream(stream, 10); err != nil {
		t.Fatalf("BuildWithRetriesFromKeyStream() error = %v", err)
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

func TestRibbonFilterBuildFromKeyStream(t *testing.T) {
	items := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}

	rf, err := NewRibbonFilter(32, 32, 24, 7)
	if err != nil {
		t.Fatalf("NewRibbonFilter() error = %v", err)
	}

	stream := func(emit func([]byte) bool) error {
		for _, item := range items {
			if !emit(item) {
				break
			}
		}
		return nil
	}

	if err := rf.BuildWithRetriesFromKeyStream(stream, 10); err != nil {
		t.Fatalf("BuildWithRetriesFromKeyStream() error = %v", err)
	}

	for _, item := range items {
		if !rf.Contains(item) {
			t.Fatalf("Contains(%q) = false, want true", string(item))
		}
	}
}

func TestRibbonFilterBuildFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keys.log")
	data := []byte("alpha\nbeta\ngamma\n")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	rf, err := NewRibbonFilter(32, 32, 24, 7)
	if err != nil {
		t.Fatalf("NewRibbonFilter() error = %v", err)
	}

	stream := func(emit func([]byte) bool) error {
		return ParseLineKeys(path, emit)
	}

	if err := rf.BuildWithRetriesFromKeyStream(stream, 10); err != nil {
		t.Fatalf("BuildWithRetriesFromKeyStream() error = %v", err)
	}

	for _, key := range []string{"alpha", "beta", "gamma"} {
		if !rf.Contains([]byte(key)) {
			t.Fatalf("Contains(%q) = false, want true", key)
		}
	}
}

func TestRibbonFilterBuildFromFileWithCustomParser(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keys.alog")
	data := []byte("1|alpha\n2|beta\n3|gamma\n")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	parser := func(path string, emit func([]byte) bool) error {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		s := bufio.NewScanner(f)
		for s.Scan() {
			parts := strings.SplitN(strings.TrimSpace(s.Text()), "|", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid alog row")
			}
			if !emit([]byte(parts[1])) {
				break
			}
		}

		return s.Err()
	}

	rf, err := NewRibbonFilter(32, 32, 24, 7)
	if err != nil {
		t.Fatalf("NewRibbonFilter() error = %v", err)
	}

	stream := func(emit func([]byte) bool) error {
		return parser(path, emit)
	}

	if err := rf.BuildWithRetriesFromKeyStream(stream, 10); err != nil {
		t.Fatalf("BuildWithRetriesFromKeyStream() error = %v", err)
	}

	for _, key := range []string{"alpha", "beta", "gamma"} {
		if !rf.Contains([]byte(key)) {
			t.Fatalf("Contains(%q) = false, want true", key)
		}
	}
}
