package segment

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func TestBuildOpenRoundTripSearches(t *testing.T) {
	data, err := Build([]TermPostings{
		{
			Term:     "alpha",
			Postings: []fts.Posting{{Ord: 1, Count: 2}, {Ord: 3, Count: 1}},
			Positions: [][]uint32{
				{0, 4},
				{2},
			},
		},
		{
			Term:     "alpine",
			Postings: []fts.Posting{{Ord: 3, Count: 3}},
		},
		{
			Term:     "beta",
			Postings: []fts.Posting{{Ord: 2, Count: 5}},
		},
	})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	r, err := Open(data)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	alpha, err := r.Search("alpha")
	if err != nil {
		t.Fatalf("Search(alpha) error = %v", err)
	}
	if len(alpha) != 2 || alpha[0].Ord != 1 || alpha[0].Seq != 1 || alpha[1].Ord != 3 {
		t.Fatalf("Search(alpha) = %+v, want ords [1 3]", alpha)
	}

	positional, err := r.SearchPositional("alpha")
	if err != nil {
		t.Fatalf("SearchPositional(alpha) error = %v", err)
	}
	if len(positional) != 2 || len(positional[0].Positions) != 2 || positional[0].Positions[1] != 4 {
		t.Fatalf("SearchPositional(alpha) = %+v, want positions [0 4] for first doc", positional)
	}

	prefix, err := r.SearchPrefix("alp")
	if err != nil {
		t.Fatalf("SearchPrefix(alp) error = %v", err)
	}
	if len(prefix) != 2 {
		t.Fatalf("len(SearchPrefix(alp)) = %d, want 2", len(prefix))
	}
	if prefix[0].Ord != 1 || prefix[0].Count != 2 {
		t.Fatalf("prefix[0] = %+v, want ord=1 count=2", prefix[0])
	}
	if prefix[1].Ord != 3 || prefix[1].Count != 4 {
		t.Fatalf("prefix[1] = %+v, want ord=3 count=4", prefix[1])
	}
}

func TestBuildRejectsUnsortedPostings(t *testing.T) {
	_, err := Build([]TermPostings{{
		Term:     "alpha",
		Postings: []fts.Posting{{Ord: 2, Count: 1}, {Ord: 1, Count: 1}},
	}})
	if err == nil {
		t.Fatal("Build() error = nil, want non-nil for unsorted postings")
	}
}

func TestOpenFileRoundTrip(t *testing.T) {
	data, err := Build([]TermPostings{{Term: "alpha", Postings: []fts.Posting{{Ord: 7, Count: 1}}}})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "segment.fidx")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	r, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	postings, err := r.Search("alpha")
	if err != nil {
		t.Fatalf("Search(alpha) error = %v", err)
	}
	if len(postings) != 1 || postings[0].Ord != 7 {
		t.Fatalf("Search(alpha) = %+v, want ord=7", postings)
	}
}

func TestOpenFileReaderRoundTripWithoutHoldingFileBytes(t *testing.T) {
	data := mustBuildSingleTerm(t, "alpha", 11)
	path := filepath.Join(t.TempDir(), "segment.fidx")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	r, err := OpenFileReader(path)
	if err != nil {
		t.Fatalf("OpenFileReader() error = %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()
	if r.Bytes() != nil {
		t.Fatal("Bytes() is non-nil for file-backed reader")
	}
	postings, err := r.Search("alpha")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(postings) != 1 || postings[0].Ord != 11 {
		t.Fatalf("Search() = %+v, want ord=11", postings)
	}
}

func TestOpenRejectsVersion2Corruption(t *testing.T) {
	data := mustBuildSingleTerm(t, "alpha", 7)
	data[headerLen+1] ^= 0xff
	if _, err := Open(data); err == nil {
		t.Fatal("Open(corrupt) error = nil, want checksum error")
	}
}

func TestOpenReadsLegacyVersion1Segment(t *testing.T) {
	legacy := toLegacyVersion1(t, mustBuildSingleTerm(t, "alpha", 13))
	r, err := Open(legacy)
	if err != nil {
		t.Fatalf("Open(v1) error = %v", err)
	}
	postings, err := r.Search("alpha")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(postings) != 1 || postings[0].Ord != 13 {
		t.Fatalf("Search() = %+v, want ord=13", postings)
	}
}

func TestSearchRejectsMalformedLegacyPostings(t *testing.T) {
	legacy := toLegacyVersion1(t, mustBuildSingleTerm(t, "alpha", 7))
	legacy[headerLen] = 2
	r, err := Open(legacy)
	if err != nil {
		t.Fatalf("Open(v1) error = %v", err)
	}
	if _, err := r.Search("alpha"); err == nil {
		t.Fatal("Search(malformed) error = nil, want decode error")
	}
}

func TestReaderIsReadOnly(t *testing.T) {
	r, err := Open(mustBuildSingleTerm(t, "alpha", 1))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := r.Insert("alpha", 1); err == nil {
		t.Fatal("Insert() error = nil, want read-only error")
	}
	if err := r.InsertAt("alpha", 0, 1); err == nil {
		t.Fatal("InsertAt() error = nil, want read-only error")
	}
}

func mustBuildSingleTerm(t *testing.T, term string, ord fts.DocOrd) []byte {
	t.Helper()
	data, err := Build([]TermPostings{{Term: term, Postings: []fts.Posting{{Ord: ord, Count: 1}}}})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	return data
}

func toLegacyVersion1(t *testing.T, current []byte) []byte {
	t.Helper()
	if len(current) < footerLen {
		t.Fatal("current segment is too short")
	}
	currentFooter := current[len(current)-footerLen:]
	legacy := append([]byte(nil), current[:len(current)-footerLen]...)
	binary.LittleEndian.PutUint16(legacy[4:6], legacyVersion)
	var footer [legacyFooterLen]byte
	copy(footer[0:16], currentFooter[0:16])
	copy(footer[16:20], magic)
	binary.LittleEndian.PutUint16(footer[20:22], legacyVersion)
	return append(legacy, footer[:]...)
}
