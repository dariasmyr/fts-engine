package ftse

import (
	"bytes"
	"context"
	"sort"

	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

// Build serializes terms as FTSE v3. Terms can be supplied in any order;
// postings within each term must be strictly ordered by ordinal.
func Build(terms []posting.TermData) ([]byte, error) {
	sorted := append([]posting.TermData(nil), terms...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Term < sorted[j].Term })
	var out bytes.Buffer
	if err := Write(context.Background(), &out, termSliceExporter(sorted), BuildOptions{}); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

type termSliceExporter []posting.TermData

func (terms termSliceExporter) ExportPostings(yield func(posting.TermData) error) error {
	for _, term := range terms {
		if err := yield(term); err != nil {
			return err
		}
	}
	return nil
}

func buildFromExporter(source posting.Exporter, options BuildOptions) ([]byte, error) {
	var out bytes.Buffer
	if err := Write(context.Background(), &out, source, options); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func ordsAreSorted(postings []posting.Posting) bool {
	for i := 1; i < len(postings); i++ {
		if postings[i].Ord <= postings[i-1].Ord {
			return false
		}
	}
	return true
}

func positionsAreSorted(positions []uint32) bool {
	for i := 1; i < len(positions); i++ {
		if positions[i] < positions[i-1] {
			return false
		}
	}
	return true
}

func anyPositions(positions [][]uint32) bool {
	for _, values := range positions {
		if len(values) > 0 {
			return true
		}
	}
	return false
}
