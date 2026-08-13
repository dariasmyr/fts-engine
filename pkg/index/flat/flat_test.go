package flat

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"reflect"
	"sync"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/segment"
)

func TestIndexInsertSearchAndCounts(t *testing.T) {
	idx := New()
	for _, ord := range []fts.DocOrd{5, 1, 5, 3, 1} {
		if err := idx.Insert("request.id", ord); err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
	}

	got, err := idx.Search("request.id")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	want := []fts.Posting{
		{Ord: 1, Count: 2, Seq: 1},
		{Ord: 3, Count: 1, Seq: 3},
		{Ord: 5, Count: 2, Seq: 5},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Search() = %+v, want %+v", got, want)
	}

	missing, err := idx.Search("missing")
	if err != nil {
		t.Fatalf("Search(missing) error = %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("Search(missing) = %+v, want no postings", missing)
	}
}

func TestSnapshotRoundTripAndChecksum(t *testing.T) {
	idx := New()
	for ord, term := range []string{"io.eof", "10.0.0.1", "io.eof"} {
		if err := idx.Insert(term, fts.DocOrd(ord)); err != nil {
			t.Fatalf("Insert(%q) error = %v", term, err)
		}
	}

	var snapshot bytes.Buffer
	if err := idx.Serialize(&snapshot); err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}
	loaded, err := Load(bytes.NewReader(snapshot.Bytes()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	got, err := loaded.Search("io.eof")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(got) != 2 || got[0].Ord != 0 || got[1].Ord != 2 {
		t.Fatalf("Search() = %+v, want ords [0 2]", got)
	}

	corrupt := append([]byte(nil), snapshot.Bytes()...)
	corrupt[len(corrupt)/2] ^= 0xff
	if _, err := Load(bytes.NewReader(corrupt)); err == nil {
		t.Fatal("Load(corrupt) error = nil, want checksum error")
	}
}

func TestLoadVersionOneSnapshot(t *testing.T) {
	payload := []byte(snapshotMagicV1)
	payload = binary.AppendUvarint(payload, 1)
	payload = binary.AppendUvarint(payload, uint64(len("hotel")))
	payload = append(payload, "hotel"...)
	payload = binary.AppendUvarint(payload, 2)
	payload = binary.AppendUvarint(payload, 1)
	payload = binary.AppendUvarint(payload, 2)
	payload = binary.AppendUvarint(payload, 4)
	payload = binary.AppendUvarint(payload, 1)
	payload = binary.LittleEndian.AppendUint32(payload, crc32.ChecksumIEEE(payload))

	loaded, err := Load(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Load(FLT1) error = %v", err)
	}
	got, err := loaded.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	want := []fts.Posting{
		{Ord: 1, Count: 2, Seq: 1},
		{Ord: 5, Count: 1, Seq: 5},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Search() = %+v, want %+v", got, want)
	}
}

func TestBuildSealedSegmentFromFlatIndex(t *testing.T) {
	idx := New()
	for _, item := range []struct {
		term string
		ord  fts.DocOrd
	}{
		{term: "trace.id", ord: 7},
		{term: "span.id", ord: 2},
		{term: "trace.id", ord: 2},
	} {
		if err := idx.Insert(item.term, item.ord); err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
	}

	data, err := segment.BuildFromSource(idx)
	if err != nil {
		t.Fatalf("BuildFromSource() error = %v", err)
	}
	sealed, err := segment.Open(data)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	got, err := sealed.Search("trace.id")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(got) != 2 || got[0].Ord != 2 || got[1].Ord != 7 {
		t.Fatalf("Search() = %+v, want ords [2 7]", got)
	}
}

func TestHighCardinalityTermsRemainSearchable(t *testing.T) {
	const terms = 10_000
	idx := New()
	for i := range terms {
		term := fmt.Sprintf("attribute.%05d", i)
		if err := idx.Insert(term, fts.DocOrd(i)); err != nil {
			t.Fatalf("Insert(%q) error = %v", term, err)
		}
	}
	for _, i := range []int{0, terms / 2, terms - 1} {
		term := fmt.Sprintf("attribute.%05d", i)
		got, err := idx.Search(term)
		if err != nil {
			t.Fatalf("Search(%q) error = %v", term, err)
		}
		if len(got) != 1 || got[0].Ord != fts.DocOrd(i) {
			t.Fatalf("Search(%q) = %+v, want ord %d", term, got, i)
		}
	}
}

func TestIndexSearchPositional(t *testing.T) {
	idx := New()
	for _, item := range []struct {
		ord fts.DocOrd
		pos uint32
	}{
		{ord: 5, pos: 4},
		{ord: 1, pos: 3},
		{ord: 5, pos: 1},
		{ord: 1, pos: 2},
	} {
		if err := idx.InsertAt("hotel", item.pos, item.ord); err != nil {
			t.Fatalf("InsertAt() error = %v", err)
		}
	}

	docs, err := idx.SearchPositional("hotel")
	if err != nil {
		t.Fatalf("SearchPositional() error = %v", err)
	}
	if len(docs) != 2 || docs[0].Ord != 1 || docs[1].Ord != 5 {
		t.Fatalf("SearchPositional() = %+v, want ords [1 5]", docs)
	}
	if !reflect.DeepEqual(docs[0].Positions, []uint32{2, 3}) ||
		!reflect.DeepEqual(docs[1].Positions, []uint32{1, 4}) {
		t.Fatalf("SearchPositional() = %+v, want sorted aligned positions", docs)
	}
	plain, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if plain[0].Count != 2 || plain[1].Count != 2 {
		t.Fatalf("Search() = %+v, want count 2 for both docs", plain)
	}
}

func TestPhraseSearchWithFlat(t *testing.T) {
	svc := fts.New(New(), fts.WordKeys)
	ctx := context.Background()
	for id, content := range map[fts.DocID]string{
		"doc-a": "hotel barge gave a speech",
		"doc-b": "barge speech today hotel was there",
		"doc-c": "hotel barge said hotel barge again",
	} {
		if err := svc.Index(ctx, fts.Document{ID: id, Fields: map[string]fts.Field{
			fts.DefaultField: {Value: content},
		}}); err != nil {
			t.Fatalf("Index(%q) error = %v", id, err)
		}
	}

	result, err := svc.SearchPhrase(ctx, "hotel barge", 10)
	if err != nil {
		t.Fatalf("SearchPhrase() error = %v", err)
	}
	hits := make(map[fts.DocID]int)
	for _, hit := range result.Results {
		hits[hit.ID] = hit.TotalMatches
	}
	if hits["doc-a"] != 1 || hits["doc-c"] != 2 {
		t.Fatalf("SearchPhrase() hits = %+v, want doc-a=1 and doc-c=2", hits)
	}
	if _, ok := hits["doc-b"]; ok {
		t.Fatalf("SearchPhrase() hits = %+v, doc-b must not match", hits)
	}
}

func TestSearchPrefix(t *testing.T) {
	idx := New()
	for _, item := range []struct {
		term string
		ord  fts.DocOrd
	}{
		{term: "barley", ord: 0},
		{term: "barley", ord: 0},
		{term: "banana", ord: 1},
		{term: "barge", ord: 2},
		{term: "market", ord: 0},
		{term: "australia", ord: 3},
	} {
		if err := idx.Insert(item.term, item.ord); err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
	}

	refs, err := idx.SearchPrefix("ba")
	if err != nil {
		t.Fatalf("SearchPrefix() error = %v", err)
	}
	want := []fts.Posting{
		{Ord: 0, Count: 2, Seq: 0},
		{Ord: 1, Count: 1, Seq: 1},
		{Ord: 2, Count: 1, Seq: 2},
	}
	if !reflect.DeepEqual(refs, want) {
		t.Fatalf("SearchPrefix() = %+v, want %+v", refs, want)
	}

	if refs, err = idx.SearchPrefix("zzz"); err != nil || len(refs) != 0 {
		t.Fatalf("SearchPrefix(zzz) = %+v, %v, want no matches", refs, err)
	}
	if refs, err = idx.SearchPrefix("barley"); err != nil || len(refs) != 1 || refs[0].Ord != 0 {
		t.Fatalf("SearchPrefix(barley) = %+v, %v, want exact-key match", refs, err)
	}
}

func TestSearchPrefixCacheSeesNewTerms(t *testing.T) {
	idx := New()
	if err := idx.Insert("alpha", 0); err != nil {
		t.Fatalf("Insert(alpha) error = %v", err)
	}
	if _, err := idx.SearchPrefix("a"); err != nil {
		t.Fatalf("SearchPrefix(a) error = %v", err)
	}
	if err := idx.Insert("alpine", 1); err != nil {
		t.Fatalf("Insert(alpine) error = %v", err)
	}
	refs, err := idx.SearchPrefix("a")
	if err != nil {
		t.Fatalf("SearchPrefix(a) error = %v", err)
	}
	if len(refs) != 2 || refs[0].Ord != 0 || refs[1].Ord != 1 {
		t.Fatalf("SearchPrefix(a) = %+v, want ords [0 1]", refs)
	}
}

func TestConcurrentInsertAndPrefixSearch(t *testing.T) {
	idx := New()
	start := make(chan struct{})
	errs := make(chan error, 5)
	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		<-start
		for i := range 1_000 {
			if err := idx.Insert(fmt.Sprintf("attribute.%04d", i), fts.DocOrd(i)); err != nil {
				errs <- err
				return
			}
		}
	}()
	for range 4 {
		go func() {
			defer wg.Done()
			<-start
			for range 50 {
				if _, err := idx.SearchPrefix("attribute.0"); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent index operation error = %v", err)
	}

	refs, err := idx.SearchPrefix("attribute.")
	if err != nil {
		t.Fatalf("SearchPrefix() error = %v", err)
	}
	if len(refs) != 1_000 || refs[0].Ord != 0 || refs[len(refs)-1].Ord != 999 {
		t.Fatalf("SearchPrefix() returned %d refs with bounds [%d,%d], want 1000 refs [0,999]",
			len(refs), refs[0].Ord, refs[len(refs)-1].Ord)
	}
}

func TestPositionalSnapshotAndSegmentRoundTrip(t *testing.T) {
	idx := New()
	for _, item := range []struct {
		ord fts.DocOrd
		pos uint32
	}{{ord: 5, pos: 1}, {ord: 1, pos: 4}, {ord: 5, pos: 3}} {
		if err := idx.InsertAt("hotel", item.pos, item.ord); err != nil {
			t.Fatalf("InsertAt() error = %v", err)
		}
	}

	var snapshot bytes.Buffer
	if err := idx.Serialize(&snapshot); err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}
	loaded, err := Load(bytes.NewReader(snapshot.Bytes()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	positional := loaded.(fts.PositionalIndex)
	refs, err := positional.SearchPositional("hotel")
	if err != nil {
		t.Fatalf("SearchPositional() error = %v", err)
	}
	if len(refs) != 2 || refs[0].Ord != 1 || !reflect.DeepEqual(refs[1].Positions, []uint32{1, 3}) {
		t.Fatalf("loaded SearchPositional() = %+v", refs)
	}

	data, err := segment.BuildFromSource(loaded.(*Index))
	if err != nil {
		t.Fatalf("BuildFromSource() error = %v", err)
	}
	sealed, err := segment.Open(data)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	refs, err = sealed.SearchPositional("hotel")
	if err != nil {
		t.Fatalf("sealed SearchPositional() error = %v", err)
	}
	if len(refs) != 2 || !reflect.DeepEqual(refs[1].Positions, []uint32{1, 3}) {
		t.Fatalf("sealed SearchPositional() = %+v", refs)
	}
}
