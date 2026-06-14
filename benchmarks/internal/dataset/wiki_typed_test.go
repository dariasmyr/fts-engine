package dataset

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/harness"
	"github.com/dariasmyr/fts-engine/benchmarks/internal/quality"
)

func TestLoadWikiTypedBuildsAllQueryGroups(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "wiki.xml")
	cacheDir := filepath.Join(tmp, "cache")
	content := `<?xml version="1.0"?>
<mediawiki>
  <page><title>Alpha</title><id>1</id><revision><text>common alpha distributed database microservice rareone</text></revision></page>
  <page><title>Beta</title><id>2</id><revision><text>common alpha systems microservices raretwo</text></revision></page>
  <page><title>Gamma</title><id>3</id><revision><text>common beta distributed database microkernel rarethree</text></revision></page>
  <page><title>Delta</title><id>4</id><revision><text>common beta systems microchip rarefour</text></revision></page>
  <page><title>Epsilon</title><id>5</id><revision><text>common gamma distributed database microscope rarefive</text></revision></page>
  <page><title>Zeta</title><id>6</id><revision><text>common gamma systems microservice raresix</text></revision></page>
</mediawiki>`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	corpus, err := LoadWikiTyped(WikiTypedConfig{
		DumpPath:        path,
		CacheDir:        cacheDir,
		K:               10,
		Seed:            42,
		QueriesPerClass: 1,
		HighSkipTop:     0,
		HighPool:        3,
		LowPool:         8,
		PrefixMinExpand: 2,
		PrefixMaxExpand: 8,
	})
	if err != nil {
		t.Fatalf("LoadWikiTyped() error = %v", err)
	}
	if len(corpus.Docs) != 6 {
		t.Fatalf("len(Docs) = %d, want 6", len(corpus.Docs))
	}
	if len(corpus.Groups) != 6 {
		t.Fatalf("len(Groups) = %d, want 6", len(corpus.Groups))
	}
	for _, group := range corpus.Groups {
		if len(group.Queries) != 1 {
			t.Fatalf("group %q query count = %d, want 1", group.Name, len(group.Queries))
		}
		q := group.Queries[0]
		if q.Class != group.Name {
			t.Fatalf("query class = %q, want %q", q.Class, group.Name)
		}
		if len(group.Qrels) != 1 {
			t.Fatalf("group %q qrels count = %d, want 1", group.Name, len(group.Qrels))
		}
		relevant := group.Qrels[q.ID]
		if len(relevant) == 0 {
			t.Fatalf("group %q relevant set is empty", group.Name)
		}
	}
	if len(corpus.Queries) != 6 {
		t.Fatalf("len(Queries) = %d, want 6", len(corpus.Queries))
	}
	if used, _ := corpus.Meta["docs_cache_used"].(bool); used {
		t.Fatal("first LoadWikiTyped() should not report docs_cache_used=true")
	}
	if used, _ := corpus.Meta["query_cache_used"].(bool); used {
		t.Fatal("first LoadWikiTyped() should not report query_cache_used=true")
	}
	manifestFile, ok := corpus.Meta["manifest_file"].(string)
	if !ok || manifestFile == "" {
		t.Fatal("manifest_file metadata should be present")
	}
	if _, err := os.Stat(manifestFile); err != nil {
		t.Fatalf("manifest_file stat error = %v", err)
	}
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatalf("ReadDir(cacheDir) error = %v", err)
	}
	if len(entries) != 4 {
		t.Fatalf("cache file count = %d, want 4", len(entries))
	}

	corpus, err = LoadWikiTyped(WikiTypedConfig{
		DumpPath:        path,
		CacheDir:        cacheDir,
		K:               10,
		Seed:            42,
		QueriesPerClass: 1,
		HighSkipTop:     0,
		HighPool:        3,
		LowPool:         8,
		PrefixMinExpand: 2,
		PrefixMaxExpand: 8,
	})
	if err != nil {
		t.Fatalf("second LoadWikiTyped() error = %v", err)
	}
	if used, _ := corpus.Meta["docs_cache_used"].(bool); !used {
		t.Fatal("second LoadWikiTyped() should report docs_cache_used=true")
	}
	if used, _ := corpus.Meta["query_cache_used"].(bool); !used {
		t.Fatal("second LoadWikiTyped() should report query_cache_used=true")
	}

	results := make([]harness.QueryResult, 0, len(corpus.Groups))
	mergedQrels := make(quality.Qrels)
	for _, group := range corpus.Groups {
		q := group.Queries[0]
		relevant := group.Qrels[q.ID]
		hits := make([]harness.SearchHit, 0, len(relevant))
		for docID := range relevant {
			hits = append(hits, harness.SearchHit{DocID: docID, Score: 1})
		}
		results = append(results, harness.QueryResult{QueryID: q.ID, Hits: hits})
		mergedQrels[q.ID] = relevant
	}
	scores := quality.Compute(results, mergedQrels, 10)
	if scores == nil {
		t.Fatal("quality.Compute() returned nil for wiki-typed qrels")
	}
	if scores.NumScored != len(corpus.Groups) {
		t.Fatalf("NumScored = %d, want %d", scores.NumScored, len(corpus.Groups))
	}
}
