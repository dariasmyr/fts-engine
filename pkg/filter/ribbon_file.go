package filter

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

const ribbonScannerMaxTokenSize = 1024 * 1024

type FileKeyParser func(path string, emit func([]byte) bool) error

func (rf *RibbonFilter) BuildFromFile(path string) error {
	return rf.BuildFromFileWithParser(path, ParseLineKeys)
}

func (rf *RibbonFilter) BuildWithRetriesFromFile(path string, maxAttempts uint32) error {
	return rf.BuildWithRetriesFromFileWithParser(path, ParseLineKeys, maxAttempts)
}

func (rf *RibbonFilter) BuildFromFileWithParser(path string, parser FileKeyParser) error {
	if parser == nil {
		return fmt.Errorf("ribbon: nil key parser")
	}

	return rf.BuildFromKeyStream(fileParserKeyStream(path, parser))
}

func (rf *RibbonFilter) BuildWithRetriesFromFileWithParser(path string, parser FileKeyParser, maxAttempts uint32) error {
	if parser == nil {
		return fmt.Errorf("ribbon: nil key parser")
	}

	return rf.BuildWithRetriesFromKeyStream(fileParserKeyStream(path, parser), maxAttempts)
}

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

func fileParserKeyStream(path string, parser FileKeyParser) func(func([]byte) bool) error {
	return func(emit func([]byte) bool) error {
		return parser(path, emit)
	}
}
