package bluge

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"
	blugeanalyzer "github.com/blugelabs/bluge/analysis/analyzer"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/harness"
)

type Adapter struct {
	cfg      bluge.Config
	writer   *bluge.Writer
	dir      string
	analyzer *analysis.Analyzer
}

func New() *Adapter { return &Adapter{} }

func (a *Adapter) Name() string { return "bluge" }

func (a *Adapter) Open(_ context.Context, dir string) error {
	a.dir = filepath.Join(dir, "bluge.index")
	if err := os.RemoveAll(a.dir); err != nil {
		return fmt.Errorf("reset bluge index dir: %w", err)
	}
	a.cfg = bluge.DefaultConfig(a.dir)
	a.analyzer = blugeanalyzer.NewStandardAnalyzer()
	w, err := bluge.OpenWriter(a.cfg)
	if err != nil {
		return fmt.Errorf("bluge.OpenWriter: %w", err)
	}
	a.writer = w
	return nil
}

func (a *Adapter) Index(_ context.Context, docs []harness.Document) error {
	batch := bluge.NewBatch()
	for _, d := range docs {
		doc := bluge.NewDocument(d.ID).
			AddField(bluge.NewTextField("body", d.Body).WithAnalyzer(a.analyzer))
		batch.Insert(doc)
	}
	return a.writer.Batch(batch)
}

func (a *Adapter) Commit(_ context.Context) error {
	if err := a.writer.Close(); err != nil {
		return err
	}
	w, err := bluge.OpenWriter(a.cfg)
	if err != nil {
		return err
	}
	a.writer = w
	return nil
}

func (a *Adapter) Search(ctx context.Context, q harness.Query) ([]harness.SearchHit, error) {
	reader, err := a.writer.Reader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	mq := bluge.NewMatchQuery(q.Text).SetField("body").SetAnalyzer(a.analyzer)
	req := bluge.NewTopNSearch(q.K, mq)
	iter, err := reader.Search(ctx, req)
	if err != nil {
		return nil, err
	}
	hits := make([]harness.SearchHit, 0, q.K)
	match, err := iter.Next()
	for err == nil && match != nil {
		var id string
		_ = match.VisitStoredFields(func(field string, value []byte) bool {
			if field == "_id" {
				id = string(value)
			}
			return true
		})
		hits = append(hits, harness.SearchHit{DocID: id, Score: match.Score})
		match, err = iter.Next()
	}
	return hits, err
}

func (a *Adapter) IndexSizeBytes() (int64, error) { return dirSize(a.dir) }

func (a *Adapter) Close() error {
	if a.writer == nil {
		return nil
	}
	err := a.writer.Close()
	a.writer = nil
	return err
}

func (a *Adapter) BenchmarkMetadata() map[string]any {
	return map[string]any{
		"analyzer": "standard",
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
