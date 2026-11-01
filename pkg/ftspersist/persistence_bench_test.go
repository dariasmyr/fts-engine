package ftspersist_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/v2/pkg/ftspersist"
	"github.com/dariasmyr/fts-engine/v2/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/v2/pkg/textproc"
)

const persistenceBenchmarkDocuments = 1_000

var persistenceBenchmarkSink int

func benchmarkPersistenceService(b *testing.B) (*fts.Service, *fts.SnapshotRegistry) {
	b.Helper()
	registry, err := ftsbuiltin.NewSnapshotRegistry()
	if err != nil {
		b.Fatalf("NewSnapshotRegistry() error = %v", err)
	}
	service := fts.New(
		slicedradix.New(),
		fts.WordKeys,
		fts.WithPipeline(textproc.DefaultEnglishPipeline()),
		fts.WithScorer(fts.BM25()),
	)
	ctx := context.Background()
	for i := range persistenceBenchmarkDocuments {
		doc := fts.Document{
			ID: fts.DocID(fmt.Sprintf("doc-%05d", i)),
			Fields: map[string]fts.Field{
				fts.DefaultField: {
					Value: fmt.Sprintf("common searchable document shard %d contains indexed persistence terms", i%100),
				},
			},
		}
		if err := service.Index(ctx, doc); err != nil {
			b.Fatalf("Index() error = %v", err)
		}
	}
	return service, registry
}

func benchmarkRestoreOptions() []*fts.Option {
	return []*fts.Option{
		fts.WithPipeline(textproc.DefaultEnglishPipeline()),
		fts.WithScorer(fts.BM25()),
	}
}

func BenchmarkSnapshotPersistence(b *testing.B) {
	service, registry := benchmarkPersistenceService(b)
	paths := ftspersist.SnapshotPaths{IndexPath: filepath.Join(b.TempDir(), "snapshot.fidx")}
	if err := ftspersist.SaveSnapshot(paths, service, "slicedradix", "", ftspersist.SaveOptions{Registry: registry}); err != nil {
		b.Fatalf("initial SaveSnapshot() error = %v", err)
	}

	b.Run("save", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := ftspersist.SaveSnapshot(paths, service, "slicedradix", "", ftspersist.SaveOptions{Registry: registry}); err != nil {
				b.Fatalf("SaveSnapshot() error = %v", err)
			}
		}
	})

	b.Run("load", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			loaded, err := ftspersist.LoadSnapshot(
				paths,
				fts.WordKeys,
				ftspersist.SnapshotLoadOptions{Registry: registry},
				benchmarkRestoreOptions()...,
			)
			if err != nil {
				b.Fatalf("LoadSnapshot() error = %v", err)
			}
			if loaded.Close != nil {
				if err := loaded.Close(); err != nil {
					b.Fatalf("Close() error = %v", err)
				}
			}
		}
	})

	benchmarkSnapshotSearch(b, paths, registry)
}

func benchmarkSnapshotSearch(b *testing.B, paths ftspersist.SnapshotPaths, registry *fts.SnapshotRegistry) {
	b.Helper()
	b.Run("search_first", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			loaded, err := ftspersist.LoadSnapshot(
				paths,
				fts.WordKeys,
				ftspersist.SnapshotLoadOptions{Registry: registry},
				benchmarkRestoreOptions()...,
			)
			if err != nil {
				b.Fatalf("LoadSnapshot() error = %v", err)
			}
			b.StartTimer()
			result, err := loaded.Service.SearchText(context.Background(), "common persistence", fts.SearchOptions{MaxResults: 10})
			if err != nil {
				b.Fatalf("SearchText() error = %v", err)
			}
			persistenceBenchmarkSink = result.TotalResultsCount
			b.StopTimer()
			if loaded.Close != nil {
				if err := loaded.Close(); err != nil {
					b.Fatalf("Close() error = %v", err)
				}
			}
			b.StartTimer()
		}
	})

	b.Run("search_warm", func(b *testing.B) {
		loaded, err := ftspersist.LoadSnapshot(
			paths,
			fts.WordKeys,
			ftspersist.SnapshotLoadOptions{Registry: registry},
			benchmarkRestoreOptions()...,
		)
		if err != nil {
			b.Fatalf("LoadSnapshot() error = %v", err)
		}
		if loaded.Close != nil {
			defer loaded.Close()
		}
		b.ReportAllocs()
		for b.Loop() {
			result, err := loaded.Service.SearchText(context.Background(), "common persistence", fts.SearchOptions{MaxResults: 10})
			if err != nil {
				b.Fatalf("SearchText() error = %v", err)
			}
			persistenceBenchmarkSink = result.TotalResultsCount
		}
	})
}

func BenchmarkSegmentPersistence(b *testing.B) {
	service, registry := benchmarkPersistenceService(b)
	paths := ftspersist.SegmentPaths{Dir: filepath.Join(b.TempDir(), "segment")}
	if err := ftspersist.SaveSegment(context.Background(), paths, service, "", ftspersist.SaveOptions{Registry: registry}); err != nil {
		b.Fatalf("initial SaveSegment() error = %v", err)
	}

	b.Run("save", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := ftspersist.SaveSegment(context.Background(), paths, service, "", ftspersist.SaveOptions{Registry: registry}); err != nil {
				b.Fatalf("SaveSegment() error = %v", err)
			}
		}
	})

	for _, access := range []ftspersist.AccessMode{ftspersist.AccessFile, ftspersist.AccessMmap} {
		b.Run("load_"+string(access), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				loaded, err := ftspersist.LoadSegment(
					paths,
					fts.WordKeys,
					ftspersist.SegmentLoadOptions{Access: access, Registry: registry},
					benchmarkRestoreOptions()...,
				)
				if err != nil {
					b.Fatalf("LoadSegment() error = %v", err)
				}
				if loaded.Close != nil {
					if err := loaded.Close(); err != nil {
						b.Fatalf("Close() error = %v", err)
					}
				}
			}
		})
		benchmarkSegmentSearch(b, paths, registry, access)
	}
}

func benchmarkSegmentSearch(b *testing.B, paths ftspersist.SegmentPaths, registry *fts.SnapshotRegistry, access ftspersist.AccessMode) {
	b.Helper()
	name := string(access)
	b.Run("search_first_"+name, func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			loaded, err := ftspersist.LoadSegment(
				paths,
				fts.WordKeys,
				ftspersist.SegmentLoadOptions{Access: access, Registry: registry},
				benchmarkRestoreOptions()...,
			)
			if err != nil {
				b.Fatalf("LoadSegment() error = %v", err)
			}
			b.StartTimer()
			result, err := loaded.Service.SearchText(context.Background(), "common persistence", fts.SearchOptions{MaxResults: 10})
			if err != nil {
				b.Fatalf("SearchText() error = %v", err)
			}
			persistenceBenchmarkSink = result.TotalResultsCount
			b.StopTimer()
			if loaded.Close != nil {
				if err := loaded.Close(); err != nil {
					b.Fatalf("Close() error = %v", err)
				}
			}
			b.StartTimer()
		}
	})

	b.Run("search_warm_"+name, func(b *testing.B) {
		loaded, err := ftspersist.LoadSegment(
			paths,
			fts.WordKeys,
			ftspersist.SegmentLoadOptions{Access: access, Registry: registry},
			benchmarkRestoreOptions()...,
		)
		if err != nil {
			b.Fatalf("LoadSegment() error = %v", err)
		}
		if loaded.Close != nil {
			defer loaded.Close()
		}
		b.ReportAllocs()
		for b.Loop() {
			result, err := loaded.Service.SearchText(context.Background(), "common persistence", fts.SearchOptions{MaxResults: 10})
			if err != nil {
				b.Fatalf("SearchText() error = %v", err)
			}
			persistenceBenchmarkSink = result.TotalResultsCount
		}
	})
}
