package ftse_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/ftse"
	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

type exporterFunc func(func(posting.TermData) error) error

func (f exporterFunc) ExportPostings(yield func(posting.TermData) error) error {
	return f(yield)
}

func TestBuildWritesDeterministicV3(t *testing.T) {
	alpha := posting.TermData{Term: "alpha", Postings: []fts.Posting{{Ord: 1, Count: 2}}}
	beta := posting.TermData{Term: "beta", Postings: []fts.Posting{{Ord: 3, Count: 1}}}
	first, err := ftse.Build([]posting.TermData{beta, alpha})
	if err != nil {
		t.Fatal(err)
	}
	second, err := ftse.Build([]posting.TermData{alpha, beta})
	if err != nil {
		t.Fatal(err)
	}
	if version := binary.LittleEndian.Uint16(first[4:6]); version != 3 {
		t.Fatalf("Build() version = %d, want 3", version)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("Build() output depends on input term order")
	}
}

func TestBuildGoldenV3Encoding(t *testing.T) {
	got, err := ftse.Build([]posting.TermData{
		{
			Term:      "alpha",
			Postings:  []posting.Posting{{Ord: 0, Count: 2}, {Ord: 129, Count: 1}},
			Positions: [][]uint32{{0, 4}, {2}},
		},
		{Term: "beta", Postings: []posting.Posting{{Ord: 7, Count: 3}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("testdata/ftse-v3.golden")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("FTSE v3 encoding differs from committed golden fixture")
	}
}

func TestWriteV3StreamsExporterAndExcludesOrdinals(t *testing.T) {
	source := exporterFunc(func(yield func(posting.TermData) error) error {
		if err := yield(posting.TermData{
			Term: "alpha",
			Postings: []posting.Posting{
				{Ord: 1, Count: 1},
				{Ord: 2, Count: 2},
			},
			Positions: [][]uint32{{3}, {5}},
		}); err != nil {
			return err
		}
		return yield(posting.TermData{Term: "beta", Postings: []posting.Posting{{Ord: 2, Count: 4}}})
	})
	var out bytes.Buffer
	err := ftse.Write(context.Background(), &out, source, ftse.BuildOptions{
		ExcludedOrdinals: []posting.Ord{2},
	})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := ftse.Open(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	alpha, err := reader.SearchPositional("alpha")
	if err != nil {
		t.Fatal(err)
	}
	if len(alpha) != 1 || alpha[0].Ord != 1 || !reflect.DeepEqual(alpha[0].Positions, []uint32{3}) {
		t.Fatalf("alpha = %+v, want ordinal 1 at position 3", alpha)
	}
	if beta, err := reader.Search("beta"); err != nil || len(beta) != 0 {
		t.Fatalf("beta = %+v, err=%v, want excluded", beta, err)
	}
}

func TestTombstoneBitmapAndOrdinalExclusionsEncodeIdentically(t *testing.T) {
	source := exporterFunc(func(yield func(posting.TermData) error) error {
		return yield(posting.TermData{
			Term:      "alpha",
			Postings:  []posting.Posting{{Ord: 1, Count: 1}, {Ord: 2, Count: 2}},
			Positions: [][]uint32{{3}, {5}},
		})
	})
	var ordinalOutput bytes.Buffer
	if err := ftse.Write(context.Background(), &ordinalOutput, source, ftse.BuildOptions{
		ExcludedOrdinals: []posting.Ord{2},
	}); err != nil {
		t.Fatal(err)
	}
	var bitmapOutput bytes.Buffer
	if err := ftse.WriteFromSourceWithTombstones(
		context.Background(),
		&bitmapOutput,
		source,
		[]uint64{1 << 2},
	); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ordinalOutput.Bytes(), bitmapOutput.Bytes()) {
		t.Fatal("ordinal and bitmap exclusions produced different FTSE artifacts")
	}
}

func TestWriteV3RejectsUnsortedExporter(t *testing.T) {
	source := exporterFunc(func(yield func(posting.TermData) error) error {
		for _, term := range []string{"beta", "alpha"} {
			if err := yield(posting.TermData{Term: term, Postings: []posting.Posting{{Ord: 1, Count: 1}}}); err != nil {
				return err
			}
		}
		return nil
	})
	if err := ftse.Write(context.Background(), &bytes.Buffer{}, source, ftse.BuildOptions{}); err == nil {
		t.Fatal("Write() accepted unsorted exporter terms")
	}
}

func TestBuildRejectsInvalidTerms(t *testing.T) {
	tests := []struct {
		name  string
		terms []posting.TermData
	}{
		{
			name:  "empty term",
			terms: []posting.TermData{{Postings: []posting.Posting{{Ord: 1, Count: 1}}}},
		},
		{
			name:  "empty postings",
			terms: []posting.TermData{{Term: "alpha"}},
		},
		{
			name: "duplicate term",
			terms: []posting.TermData{
				{Term: "alpha", Postings: []posting.Posting{{Ord: 1, Count: 1}}},
				{Term: "alpha", Postings: []posting.Posting{{Ord: 2, Count: 1}}},
			},
		},
		{
			name:  "zero frequency",
			terms: []posting.TermData{{Term: "alpha", Postings: []posting.Posting{{Ord: 1}}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ftse.Build(test.terms); err == nil {
				t.Fatal("Build() accepted invalid term data")
			}
		})
	}
}

func TestWriteV3HonorsCanceledContextBeforeOutput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	err := ftse.Write(ctx, &out, exporterFunc(func(func(posting.TermData) error) error {
		return nil
	}), ftse.BuildOptions{})
	if !errors.Is(err, context.Canceled) || out.Len() != 0 {
		t.Fatalf("Write() len=%d error=%v, want no output and context.Canceled", out.Len(), err)
	}
}

func TestV3RoundTripAndSparseAdvance(t *testing.T) {
	postings := make([]fts.Posting, 300)
	positions := make([][]uint32, len(postings))
	for i := range postings {
		postings[i] = fts.Posting{Ord: fts.DocOrd(i * 3), Count: uint32(i%5 + 1)}
		positions[i] = []uint32{uint32(i), uint32(i + 2)}
	}
	blob, err := ftse.Build([]posting.TermData{
		{Term: "alpha", Postings: postings, Positions: positions},
		{Term: "single", Postings: []fts.Posting{{Ord: 7, Count: 2}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := ftse.Open(blob)
	if err != nil {
		t.Fatal(err)
	}

	got, err := reader.Search("alpha")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, postings) {
		t.Fatalf("Search(alpha) mismatch\ngot=%v\nwant=%v", got, postings)
	}
	positional, err := reader.SearchPositional("alpha")
	if err != nil {
		t.Fatal(err)
	}
	if len(positional) != len(postings) || !reflect.DeepEqual(positional[129].Positions, positions[129]) {
		t.Fatalf("SearchPositional(alpha) mismatch at block boundary: %+v", positional[129])
	}

	cursor, count, err := reader.OpenFrequencyPostings(context.Background(), "alpha")
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if count != uint64(len(postings)) {
		t.Fatalf("document frequency = %d, want %d", count, len(postings))
	}
	if !cursor.Advance(500) || cursor.Ord() != 501 || cursor.Frequency() != postings[167].Count {
		t.Fatalf("Advance(500) ord=%d frequency=%d err=%v", cursor.Ord(), cursor.Frequency(), cursor.Err())
	}
	if !cursor.Advance(501) || cursor.Ord() != 501 {
		t.Fatalf("Advance must retain current ordinal, got %d", cursor.Ord())
	}
}

func TestV3PostingChecksumIsLazy(t *testing.T) {
	blob, err := ftse.Build([]posting.TermData{{
		Term: "alpha",
		Postings: []fts.Posting{
			{Ord: 1, Count: 1},
			{Ord: 5, Count: 2},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := ftse.Open(blob)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Search("alpha"); err != nil {
		t.Fatal(err)
	}

	corrupt := append([]byte(nil), blob...)
	corrupt[12] ^= 0xff // first posting block; metadata and dictionary stay valid
	reader, err = ftse.Open(corrupt)
	if err != nil {
		t.Fatalf("Open() checked data eagerly: %v", err)
	}
	if _, err := reader.Search("alpha"); err == nil {
		t.Fatal("Search() accepted a corrupt posting block")
	}
}

func TestV3PositionChecksumIsLazy(t *testing.T) {
	blob, err := ftse.Build([]posting.TermData{{
		Term:      "alpha",
		Postings:  []fts.Posting{{Ord: 1, Count: 1}},
		Positions: [][]uint32{{3}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), blob...)
	corrupt[14] ^= 0xff // posting block is bytes [12:14], followed by positions
	reader, err := ftse.Open(corrupt)
	if err != nil {
		t.Fatalf("Open() checked position data eagerly: %v", err)
	}
	if _, err := reader.Search("alpha"); err != nil {
		t.Fatalf("Search() read position data: %v", err)
	}
	if _, err := reader.SearchPositional("alpha"); err == nil {
		t.Fatal("SearchPositional() accepted a corrupt position block")
	}
	if err := reader.VerifyAll(context.Background()); err == nil {
		t.Fatal("VerifyAll() accepted a corrupt position block")
	}
}

func TestV3VerifyAllHonorsContext(t *testing.T) {
	blob, err := ftse.Build([]posting.TermData{{
		Term:     "alpha",
		Postings: []fts.Posting{{Ord: 1, Count: 1}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := ftse.Open(blob)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := reader.VerifyAll(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("VerifyAll() error = %v, want context.Canceled", err)
	}
}

func TestV3ConcurrentCursors(t *testing.T) {
	postings := make([]fts.Posting, 400)
	for i := range postings {
		postings[i] = fts.Posting{Ord: fts.DocOrd(i * 2), Count: uint32(i%3 + 1)}
	}
	blob, err := ftse.Build([]posting.TermData{{Term: "alpha", Postings: postings}})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := ftse.Open(blob)
	if err != nil {
		t.Fatal(err)
	}

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for worker := range workers {
		wg.Add(1)
		go func(target fts.DocOrd) {
			defer wg.Done()
			cursor, _, err := reader.OpenFrequencyPostings(context.Background(), "alpha")
			if err != nil {
				errs <- err
				return
			}
			defer cursor.Close()
			if !cursor.Advance(target) || cursor.Ord() < target {
				errs <- errors.New("cursor did not advance to target")
				return
			}
			for cursor.Next() {
			}
			if err := cursor.Err(); err != nil {
				errs <- err
			}
		}(fts.DocOrd(worker * 57))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
