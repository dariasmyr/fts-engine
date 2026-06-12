package bleve

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	bleveapi "github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"

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
	bq, err := a.buildQuery(q)
	if err != nil {
		return nil, err
	}
	req := bleveapi.NewSearchRequestOptions(bq, q.K, 0, false)
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

func (a *Adapter) buildQuery(q harness.Query) (query.Query, error) {
	switch q.Kind {
	case "", harness.QueryKindText, harness.QueryKindTerm:
		mq := bleveapi.NewMatchQuery(q.Text)
		mq.SetField("body")
		return mq, nil
	case harness.QueryKindPhrase:
		pq := bleveapi.NewMatchPhraseQuery(q.Text)
		pq.SetField("body")
		return pq, nil
	case harness.QueryKindPrefix:
		pq := bleveapi.NewPrefixQuery(strings.ToLower(q.Text))
		pq.SetField("body")
		return pq, nil
	case harness.QueryKindBoolean:
		return buildBleveBooleanQuery(q.Boolean)
	default:
		return nil, fmt.Errorf("unsupported harness query kind %q", q.Kind)
	}
}

func buildBleveBooleanQuery(spec *harness.BoolQuery) (query.Query, error) {
	bq := bleveapi.NewBooleanQuery()
	if spec == nil {
		return bq, nil
	}
	for _, clause := range spec.Clauses {
		child, err := buildBleveAtom(clause.Atom)
		if err != nil {
			return nil, err
		}
		switch clause.Occur {
		case harness.OccurMust:
			bq.AddMust(child)
		case harness.OccurShould:
			bq.AddShould(child)
		case harness.OccurMustNot:
			bq.AddMustNot(child)
		default:
			return nil, fmt.Errorf("unsupported bool occur %q", clause.Occur)
		}
	}
	return bq, nil
}

func buildBleveAtom(atom harness.Atom) (query.Query, error) {
	switch atom.Kind {
	case "", harness.QueryKindText, harness.QueryKindTerm:
		mq := bleveapi.NewMatchQuery(atom.Text)
		mq.SetField("body")
		return mq, nil
	case harness.QueryKindPhrase:
		pq := bleveapi.NewMatchPhraseQuery(atom.Text)
		pq.SetField("body")
		return pq, nil
	case harness.QueryKindPrefix:
		pq := bleveapi.NewPrefixQuery(strings.ToLower(atom.Text))
		pq.SetField("body")
		return pq, nil
	default:
		return nil, fmt.Errorf("unsupported bool atom kind %q", atom.Kind)
	}
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
