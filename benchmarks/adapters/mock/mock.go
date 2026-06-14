package mock

import (
	"context"
	"sort"
	"strings"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/harness"
)

type Adapter struct {
	postings map[string]map[string]struct{}
}

func New() *Adapter { return &Adapter{} }

func (a *Adapter) Name() string { return "mock" }

func (a *Adapter) Open(_ context.Context, _ string) error {
	a.postings = make(map[string]map[string]struct{})
	return nil
}

func (a *Adapter) Index(_ context.Context, docs []harness.Document) error {
	for _, doc := range docs {
		seen := make(map[string]struct{})
		for _, term := range strings.Fields(doc.Body) {
			if _, ok := seen[term]; ok {
				continue
			}
			seen[term] = struct{}{}
			if a.postings[term] == nil {
				a.postings[term] = make(map[string]struct{})
			}
			a.postings[term][doc.ID] = struct{}{}
		}
	}
	return nil
}

func (a *Adapter) Commit(context.Context) error { return nil }

func (a *Adapter) Search(_ context.Context, q harness.Query) ([]harness.SearchHit, error) {
	scores := make(map[string]float64)
	for _, term := range strings.Fields(q.Text) {
		for docID := range a.postings[term] {
			scores[docID]++
		}
	}
	hits := make([]harness.SearchHit, 0, len(scores))
	for docID, score := range scores {
		hits = append(hits, harness.SearchHit{DocID: docID, Score: score})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].DocID < hits[j].DocID
	})
	if q.K > 0 && len(hits) > q.K {
		hits = hits[:q.K]
	}
	return hits, nil
}

func (a *Adapter) IndexSizeBytes() (int64, error) { return 0, nil }

func (a *Adapter) Close() error {
	a.postings = nil
	return nil
}
