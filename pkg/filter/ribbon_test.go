package filter

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"errors"
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

func TestRibbonFilterRejectsUnbuiltPersistence(t *testing.T) {
	var nilFilter *RibbonFilter
	if !nilFilter.Contains([]byte("anything")) {
		t.Fatal("nil filter must fail open")
	}

	rf, err := NewRibbonFilter(8, 8, 8, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !rf.Contains([]byte("anything")) {
		t.Fatal("unbuilt filter must fail open")
	}
	if err := rf.Serialize(&bytes.Buffer{}); err == nil {
		t.Fatal("Serialize() error = nil for unbuilt filter")
	}

	var legacy bytes.Buffer
	if err := gob.NewEncoder(&legacy).Encode(ribbonSnapshot{M: 16, W: 8, Span: 9, Cells: make([]uint16, 16)}); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRibbonFilter(bytes.NewReader(legacy.Bytes())); err == nil {
		t.Fatal("LoadRibbonFilter() error = nil for unbuilt legacy filter")
	}
}

func TestNewRibbonFilterRejectsCellCountOverflow(t *testing.T) {
	if _, err := NewRibbonFilter(^uint32(0), ^uint32(0), 32, 1); err == nil {
		t.Fatal("NewRibbonFilter() error = nil for overflowing cell count")
	}
}

func TestRibbonFilterRejectsCorruptV2(t *testing.T) {
	rf, err := NewRibbonFilter(32, 32, 24, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := rf.BuildWithRetriesFromKeyStream(func(emit func([]byte) bool) error {
		emit([]byte("alpha"))
		return nil
	}, 10); err != nil {
		t.Fatal(err)
	}
	var payload bytes.Buffer
	if err := rf.Serialize(&payload); err != nil {
		t.Fatal(err)
	}
	data := payload.Bytes()
	data[len(data)/2] ^= 0xff
	if _, err := LoadRibbonFilter(bytes.NewReader(data)); err == nil {
		t.Fatal("LoadRibbonFilter() error = nil for corrupt snapshot")
	}
}

func TestRibbonFilterLoadsBuiltLegacySnapshot(t *testing.T) {
	rf, err := NewRibbonFilter(32, 32, 24, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := rf.BuildWithRetriesFromKeyStream(func(emit func([]byte) bool) error {
		emit([]byte("alpha"))
		return nil
	}, 10); err != nil {
		t.Fatal(err)
	}
	var legacy bytes.Buffer
	if err := gob.NewEncoder(&legacy).Encode(ribbonSnapshot{
		M: rf.m, W: rf.w, Seed: rf.seed, Span: rf.span,
		Cells: append([]uint16(nil), rf.cells...), Built: true,
	}); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadRibbonFilter(bytes.NewReader(legacy.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Contains([]byte("alpha")) {
		t.Fatal("legacy round trip lost membership")
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

	stream := func(emit func([]byte) bool) (streamErr error) {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() { streamErr = errors.Join(streamErr, f.Close()) }()

		s := bufio.NewScanner(f)
		for s.Scan() {
			key := strings.TrimSpace(s.Text())
			if key == "" {
				continue
			}
			if !emit([]byte(key)) {
				break
			}
		}
		return s.Err()
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

	parser := func(path string, emit func([]byte) bool) (parseErr error) {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() { parseErr = errors.Join(parseErr, f.Close()) }()

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
