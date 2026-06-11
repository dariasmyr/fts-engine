package bleve

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	bleveapi "github.com/blevesearch/bleve/v2"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/harness"
)

type Adapter struct {
	idx bleveapi.Index
	dir string
}

func New() *Adapter { return &Adapter{} }

func (a *Adapter) Name() string { return "bleve" }

func (a *Adapter) Open(_ context.Context, dir string) error {
	a.dir = filepath.Join(dir, "bleve.index")
	if err := os.RemoveAll(a.dir); err != nil {
		return fmt.Errorf("reset bleve index dir: %w", err)
	}
	mapping := bleveapi.NewIndexMapping()
	mapping.DefaultAnalyzer = "en"
	idx, err := bleveapi.New(a.dir, mapping)
	if err != nil {
		return fmt.Errorf("bleve.New: %w", err)
	}
	a.idx = idx
	return nil
}

func (a *Adapter) Index(_ context.Context, docs []harness.Document) error {
	batch := a.idx.NewBatch()
	for _, d := range docs {
		if err := batch.Index(d.ID, map[string]any{"body": d.Body}); err != nil {
			return err
		}
	}
	return a.idx.Batch(batch)
}

func (a *Adapter) Commit(_ context.Context) error { return nil }

func (a *Adapter) Search(_ context.Context, q harness.Query) ([]harness.SearchHit, error) {
	mq := bleveapi.NewMatchQuery(q.Text)
	mq.SetField("body")
	req := bleveapi.NewSearchRequestOptions(mq, q.K, 0, false)
	res, err := a.idx.Search(req)
	if err != nil {
		return nil, err
	}
	hits := make([]harness.SearchHit, 0, len(res.Hits))
	for _, hit := range res.Hits {
		hits = append(hits, harness.SearchHit{DocID: hit.ID, Score: hit.Score})
	}
	return hits, nil
}

func (a *Adapter) IndexSizeBytes() (int64, error) { return dirSize(a.dir) }

func (a *Adapter) Close() error {
	if a.idx == nil {
		return nil
	}
	err := a.idx.Close()
	a.idx = nil
	return err
}

func (a *Adapter) BenchmarkMetadata() map[string]any {
	return map[string]any{
		"analyzer": "en",
		"mode":     "disk",
		"field":    "body",
	}
}

func dirSize(path string) (int64, error) {
	var total int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}
