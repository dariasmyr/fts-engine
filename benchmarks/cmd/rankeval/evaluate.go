package main

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftseval"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func evaluate(ctx context.Context, docs []fts.Document, queries []ftseval.Query, k int, opts ...fts.Option) (*ftseval.Report, error) {
	engine := fts.NewMultiField(func(string) (fts.Index, error) {
		return slicedradix.New(), nil
	}, keygen.Word, opts...)
	for _, doc := range docs {
		if err := engine.Index(ctx, doc); err != nil {
			return nil, err
		}
	}
	return ftseval.Evaluate(ctx, queries, k, func(ctx context.Context, query string, k int) ([]fts.DocID, error) {
		res, err := engine.SearchDocuments(ctx, query, k)
		if err != nil {
			return nil, err
		}
		ids := make([]fts.DocID, 0, len(res.Results))
		for _, item := range res.Results {
			ids = append(ids, item.ID)
		}
		return ids, nil
	})
}
