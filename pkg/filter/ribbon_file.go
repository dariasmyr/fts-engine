package filter

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

const ribbonScannerMaxTokenSize = 1024 * 1024

type FileKeyParser func(path string, emit func([]byte) bool) error

// ParseLineKeys parses plain text files where each non-empty line is one key.
func ParseLineKeys(path string, emit func([]byte) bool) error {
	if path == "" {
		return fmt.Errorf("ribbon: empty key file path")
	}

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("ribbon: open key file: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, ribbonScannerMaxTokenSize)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if !emit([]byte(line)) {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("ribbon: scan key file: %w", err)
	}

	return nil
}
