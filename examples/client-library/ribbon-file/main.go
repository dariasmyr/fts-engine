package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dariasmyr/fts-engine/pkg/filter"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
	"github.com/dariasmyr/fts-engine/pkg/textproc"
)

func main() {
	tmpDir, err := os.MkdirTemp("", "fts-ribbon-example-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	if err = runDefaultParserSave(tmpDir); err != nil {
		panic(err)
	}

	if err = runCustomParserSave(tmpDir); err != nil {
		panic(err)
	}

	if err = runLoadFromFile(tmpDir); err != nil {
		panic(err)
	}
}

// runDefaultParserSave builds ribbon from plain text keys (1 key per line)
// and saves the filter snapshot to file.
func runDefaultParserSave(tmpDir string) error {
	plainPath := filepath.Join(tmpDir, "keys-default.txt")
	snapshotPath := filepath.Join(tmpDir, "ribbon-default.filter.fidx")

	if err := writeLines(plainPath, []string{"alpha", "beta", "gamma", "delta"}); err != nil {
		return err
	}

	rf, err := filter.NewRibbonFilter(16, 8, 24, 0)
	if err != nil {
		return err
	}

	if err = rf.BuildWithRetriesFromFile(plainPath, 5); err != nil {
		return err
	}

	if err = saveRibbonToFile(rf, snapshotPath); err != nil {
		return err
	}

	fmt.Printf("case=default-save file=%s contains(alpha)=%v\n", snapshotPath, rf.Contains([]byte("alpha")))
	return nil
}

// runCustomParserSave builds ribbon from a custom formatted file
// and saves the filter snapshot to file.
func runCustomParserSave(tmpDir string) error {
	csvPath := filepath.Join(tmpDir, "keys-custom.csv")
	snapshotPath := filepath.Join(tmpDir, "ribbon-custom.filter.fidx")

	if err := writeLines(csvPath, []string{"1,hotel", "2,market", "3,travel", "4,booking"}); err != nil {
		return err
	}

	rf, err := filter.NewRibbonFilter(16, 8, 24, 42)
	if err != nil {
		return err
	}

	if err = rf.BuildWithRetriesFromFileWithParser(csvPath, parseCSVSecondColumn, 5); err != nil {
		return err
	}

	if err = saveRibbonToFile(rf, snapshotPath); err != nil {
		return err
	}

	fmt.Printf("case=custom-save file=%s contains(market)=%v\n", snapshotPath, rf.Contains([]byte("market")))
	return nil
}

// runLoadFromFile loads ribbon snapshot from disk and validates Contains.
func runLoadFromFile(tmpDir string) error {
	snapshotPath := filepath.Join(tmpDir, "ribbon-default.filter.fidx")

	loaded, err := loadRibbonFromFile(snapshotPath)
	if err != nil {
		return err
	}

	rawOK := loaded.Contains([]byte("GaMmA"))

	pipe := textproc.NewPipeline(textproc.AlnumTokenizer{}, textproc.LowercaseFilter{})
	normalizedOK, err := fts.ContainsNormalized(loaded, "GaMmA", pipe, keygen.Word)
	if err != nil {
		return err
	}

	fmt.Printf("case=load file=%s contains(gamma)=%v contains(omega)=%v rawContains(\"GaMmA\")=%v normalizedContains(\"GaMmA\")=%v\n", snapshotPath, loaded.Contains([]byte("gamma")), loaded.Contains([]byte("omega")), rawOK, normalizedOK)
	return nil
}

func parseCSVSecondColumn(path string, emit func([]byte) bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, ",", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[1])
		if key == "" {
			continue
		}

		if !emit([]byte(key)) {
			break
		}
	}

	return scanner.Err()
}

func saveRibbonToFile(rf *filter.RibbonFilter, path string) error {
	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer out.Close()

	return rf.Serialize(out)
}

func loadRibbonFromFile(path string) (*filter.RibbonFilter, error) {
	in, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer in.Close()

	return filter.LoadRibbonFilter(in)
}

func writeLines(path string, lines []string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, line := range lines {
		if _, err = w.WriteString(line + "\n"); err != nil {
			return err
		}
	}

	return w.Flush()
}
