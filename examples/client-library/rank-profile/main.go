package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func main() {
	ctx := context.Background()
	docs := []fts.Document{
		{
			ID: "doc-title",
			Fields: map[string]fts.Field{
				"title":    {Value: "postgres backup"},
				"abstract": {Value: "short guide"},
			},
		},
		{
			ID: "doc-abstract",
			Fields: map[string]fts.Field{
				"title":    {Value: "database guide"},
				"abstract": {Value: "postgres backup postgres backup postgres backup"},
			},
		},
	}

	baseline := newEngine(fts.WithScorer(fts.BM25()))
	weighted := newEngine(fts.WithRankProfile(fts.RankProfile{
		Name: "docs",
		Base: fts.BM25(),
		FieldWeights: fts.FieldWeights{
			"title":    3.0,
			"abstract": 1.0,
		},
	}))

	for _, engine := range []*fts.Service{baseline, weighted} {
		for _, doc := range docs {
			if err := engine.Index(ctx, doc); err != nil {
				panic(err)
			}
		}
	}

	printResults(ctx, "baseline BM25", baseline)
	printResults(ctx, "rank profile title=3 abstract=1", weighted)
}

func newEngine(opts ...fts.Option) *fts.Service {
	return fts.NewMultiField(func(string) (fts.Index, error) {
		return slicedradix.New(), nil
	}, keygen.Word, opts...)
}

func printResults(ctx context.Context, label string, engine *fts.Service) {
	res, err := engine.SearchDocuments(ctx, "postgres backup", 10)
	if err != nil {
		panic(err)
	}

	fmt.Println(label)
	for i, item := range res.Results {
		fmt.Printf("%d. id=%s score=%.4f unique=%d total=%d\n", i+1, item.ID, item.Score, item.UniqueMatches, item.TotalMatches)
	}
	fmt.Println()
}
