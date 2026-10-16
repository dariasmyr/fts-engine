package ftspersist

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

func TestSaveSegmentHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dir := filepath.Join(t.TempDir(), "segment")
	err := SaveSegment(ctx, SegmentPaths{Dir: dir}, fts.New(newMemoryExportIndex(), fts.WordKeys), "", SaveOptions{Registry: fts.NewSnapshotRegistry()})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SaveSegment() error = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(filepath.Join(dir, segmentCurrentFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("CURRENT exists after canceled save: %v", err)
	}
}

func TestSaveSegmentPublishesImmutableGenerations(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	svc := fts.New(newMemoryExportIndex(), fts.WordKeys)
	paths := SegmentPaths{Dir: filepath.Join(t.TempDir(), "segment")}
	if err := svc.Index(context.Background(), fts.Document{ID: "one", Fields: map[string]fts.Field{fts.DefaultField: {Value: "first"}}}); err != nil {
		t.Fatal(err)
	}
	if err := SaveSegment(context.Background(), paths, svc, "", SaveOptions{Registry: registry}); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(filepath.Join(paths.Dir, segmentCurrentFile))
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Index(context.Background(), fts.Document{ID: "two", Fields: map[string]fts.Field{fts.DefaultField: {Value: "second"}}}); err != nil {
		t.Fatal(err)
	}
	if err := SaveSegment(context.Background(), paths, svc, "", SaveOptions{Registry: registry}); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(filepath.Join(paths.Dir, segmentCurrentFile))
	if err != nil {
		t.Fatal(err)
	}
	if string(first) == string(second) {
		t.Fatal("CURRENT did not advance to a new immutable generation")
	}
	loaded, err := LoadSegment(paths, fts.WordKeys, SegmentLoadOptions{Registry: registry})
	if err != nil {
		t.Fatalf("LoadSegment() error = %v", err)
	}
	defer loaded.Close()
	result, err := loaded.Service.SearchText(context.Background(), "second", fts.SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatal(err)
	}
	if result.TotalResultsCount != 1 || result.Results[0].ID != "two" {
		t.Fatalf("second generation results = %+v", result.Results)
	}
}

func TestSaveSegmentPrunesSupersededGenerations(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	svc := fts.New(newMemoryExportIndex(), fts.WordKeys)
	paths := SegmentPaths{Dir: filepath.Join(t.TempDir(), "segment")}

	var published []string
	for i, term := range []string{"first", "second", "third"} {
		id := fts.DocID(fmt.Sprintf("doc-%d", i))
		if err := svc.Index(context.Background(), fts.Document{
			ID:     id,
			Fields: map[string]fts.Field{fts.DefaultField: {Value: term}},
		}); err != nil {
			t.Fatal(err)
		}
		if err := SaveSegment(context.Background(), paths, svc, "", SaveOptions{Registry: registry}); err != nil {
			t.Fatal(err)
		}
		current, err := os.ReadFile(filepath.Join(paths.Dir, segmentCurrentFile))
		if err != nil {
			t.Fatal(err)
		}
		published = append(published, strings.TrimSpace(string(current)))
	}

	entries, err := os.ReadDir(filepath.Join(paths.Dir, segmentGenerationsDir))
	if err != nil {
		t.Fatal(err)
	}
	generations := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "generation-") {
			generations = append(generations, entry.Name())
		}
	}
	slices.Sort(generations)
	want := []string{published[1], published[2]}
	slices.Sort(want)
	if !slices.Equal(generations, want) {
		t.Fatalf("generations = %v, want current and previous %v", generations, want)
	}

	loaded, err := LoadSegment(paths, fts.WordKeys, SegmentLoadOptions{Registry: registry})
	if err != nil {
		t.Fatalf("LoadSegment() error = %v", err)
	}
	defer loaded.Close()
	result, err := loaded.Service.SearchText(context.Background(), "third", fts.SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("SearchText() error = %v", err)
	}
	if result.TotalResultsCount != 1 || result.Results[0].ID != "doc-2" {
		t.Fatalf("current generation results = %+v", result.Results)
	}
}

func TestSaveSegmentKeepsLoadedGenerationUntilClose(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	svc := fts.New(newMemoryExportIndex(), fts.WordKeys)
	paths := SegmentPaths{Dir: filepath.Join(t.TempDir(), "segment")}
	saveTerm := func(id, term string) string {
		t.Helper()
		if err := svc.Index(context.Background(), fts.Document{
			ID:     fts.DocID(id),
			Fields: map[string]fts.Field{fts.DefaultField: {Value: term}},
		}); err != nil {
			t.Fatal(err)
		}
		if err := SaveSegment(context.Background(), paths, svc, "", SaveOptions{Registry: registry}); err != nil {
			t.Fatal(err)
		}
		current, err := os.ReadFile(filepath.Join(paths.Dir, segmentCurrentFile))
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(current))
	}

	first := saveTerm("doc-1", "first")
	loaded, err := LoadSegment(paths, fts.WordKeys, SegmentLoadOptions{Registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	saveTerm("doc-2", "second")
	saveTerm("doc-3", "third")
	firstPath := filepath.Join(paths.Dir, segmentGenerationsDir, first)
	if _, err := os.Stat(firstPath); err != nil {
		t.Fatalf("loaded generation was pruned: %v", err)
	}
	if err := loaded.Close(); err != nil {
		t.Fatal(err)
	}
	saveTerm("doc-4", "fourth")
	if _, err := os.Stat(firstPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("closed generation still exists: %v", err)
	}
}

func TestSaveSegmentRecoversMalformedCurrent(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	svc := fts.New(newMemoryExportIndex(), fts.WordKeys)
	paths := SegmentPaths{Dir: filepath.Join(t.TempDir(), "segment")}
	if err := svc.Index(context.Background(), fts.Document{ID: "one", Fields: map[string]fts.Field{fts.DefaultField: {Value: "first"}}}); err != nil {
		t.Fatal(err)
	}
	if err := SaveSegment(context.Background(), paths, svc, "", SaveOptions{Registry: registry}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(paths.Dir, segmentCurrentFile), []byte("malformed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := svc.Index(context.Background(), fts.Document{ID: "two", Fields: map[string]fts.Field{fts.DefaultField: {Value: "second"}}}); err != nil {
		t.Fatal(err)
	}
	if err := SaveSegment(context.Background(), paths, svc, "", SaveOptions{Registry: registry}); err != nil {
		t.Fatalf("SaveSegment() recovery error = %v", err)
	}
	loaded, err := LoadSegment(paths, fts.WordKeys, SegmentLoadOptions{Registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	defer loaded.Close()
	result, err := loaded.Service.SearchText(context.Background(), "second", fts.SearchOptions{MaxResults: 10})
	if err != nil || result.TotalResultsCount != 1 {
		t.Fatalf("recovered generation search = %+v, error = %v", result, err)
	}
}

func TestSaveSegmentSerializesConcurrentWriters(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	svc := fts.New(newMemoryExportIndex(), fts.WordKeys)
	paths := SegmentPaths{Dir: filepath.Join(t.TempDir(), "segment")}
	if err := svc.Index(context.Background(), fts.Document{ID: "one", Fields: map[string]fts.Field{fts.DefaultField: {Value: "shared"}}}); err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, 2)
	var writers sync.WaitGroup
	for range 2 {
		writers.Go(func() {
			errs <- SaveSegment(context.Background(), paths, svc, "", SaveOptions{Registry: registry})
		})
	}
	writers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("SaveSegment() error = %v", err)
		}
	}
	loaded, err := LoadSegment(paths, fts.WordKeys, SegmentLoadOptions{Registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	defer loaded.Close()
}

func TestLoadedSegmentCloseIsConcurrentSafe(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	svc := fts.New(newMemoryExportIndex(), fts.WordKeys)
	paths := SegmentPaths{Dir: filepath.Join(t.TempDir(), "segment")}
	if err := svc.Index(context.Background(), fts.Document{ID: "one", Fields: map[string]fts.Field{fts.DefaultField: {Value: "shared"}}}); err != nil {
		t.Fatal(err)
	}
	if err := SaveSegment(context.Background(), paths, svc, "", SaveOptions{Registry: registry}); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadSegment(paths, fts.WordKeys, SegmentLoadOptions{Registry: registry})
	if err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, 4)
	var closers sync.WaitGroup
	for range 4 {
		closers.Go(func() { errs <- loaded.Close() })
	}
	closers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}
}

type memoryExportIndex struct {
	mu    sync.RWMutex
	terms map[string][]posting.Posting
}

func newMemoryExportIndex() *memoryExportIndex {
	return &memoryExportIndex{terms: make(map[string][]posting.Posting)}
}

func (i *memoryExportIndex) Insert(key string, ord fts.DocOrd) error {
	i.mu.Lock()
	i.terms[key] = append(i.terms[key], posting.Posting{Ord: ord, Count: 1})
	i.mu.Unlock()
	return nil
}

func (i *memoryExportIndex) Search(string) ([]fts.Posting, error) { return nil, nil }

func (i *memoryExportIndex) ExportPostings(yield func(posting.TermData) error) error {
	i.mu.RLock()
	defer i.mu.RUnlock()
	keys := make([]string, 0, len(i.terms))
	for key := range i.terms {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		values := i.terms[key]
		if err := yield(posting.TermData{Term: key, Postings: values}); err != nil {
			return err
		}
	}
	return nil
}
