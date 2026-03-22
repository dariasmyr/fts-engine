package filter

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRibbonBuildFromFileDefaultParser50Keys(t *testing.T) {
	keys := makeTestKeys(50)
	path := filepath.Join(t.TempDir(), "keys_default_50.txt")

	var b strings.Builder
	for _, key := range keys {
		b.WriteString(key)
		b.WriteByte('\n')
	}

	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	rf, err := NewRibbonFilter(50, 25, 24, 11)
	if err != nil {
		t.Fatalf("NewRibbonFilter() error = %v", err)
	}

	if err := rf.BuildWithRetriesFromFile(path, 10); err != nil {
		t.Fatalf("BuildWithRetriesFromFile() error = %v", err)
	}

	for _, key := range keys {
		if !rf.Contains([]byte(key)) {
			t.Fatalf("Contains(%q) = false, want true", key)
		}
	}
}

func TestRibbonBuildFromFileCustomParser50Keys(t *testing.T) {
	keys := makeTestKeys(50)
	path := filepath.Join(t.TempDir(), "keys_custom_50.alog")

	var b strings.Builder
	for i, key := range keys {
		b.WriteString(fmt.Sprintf("%d|%s\n", i+1, key))
	}

	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
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
			line := strings.TrimSpace(s.Text())
			if line == "" {
				continue
			}

			parts := strings.SplitN(line, "|", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid row: %q", line)
			}

			if !emit([]byte(parts[1])) {
				break
			}
		}

		return s.Err()
	}

	rf, err := NewRibbonFilter(50, 25, 24, 22)
	if err != nil {
		t.Fatalf("NewRibbonFilter() error = %v", err)
	}

	if err := rf.BuildWithRetriesFromFileWithParser(path, parser, 10); err != nil {
		t.Fatalf("BuildWithRetriesFromFileWithParser() error = %v", err)
	}

	for _, key := range keys {
		if !rf.Contains([]byte(key)) {
			t.Fatalf("Contains(%q) = false, want true", key)
		}
	}
}

func makeTestKeys(n int) []string {
	keys := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		keys = append(keys, fmt.Sprintf("key-%03d", i))
	}
	return keys
}
