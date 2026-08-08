package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func main() {
	engine := fts.NewMultiField(func(string) (fts.Index, error) {
		return slicedradix.New(), nil
	}, keygen.Word)

	ctx := context.Background()
	index := func(id string, fields map[string]string) {
		docFields := make(map[string]fts.Field, len(fields))
		for name, value := range fields {
			docFields[name] = fts.Field{Value: value}
		}
		if err := engine.Index(ctx, fts.Document{ID: fts.DocID(id), Fields: docFields}); err != nil {
			panic(err)
		}
	}

	index("doc-1", map[string]string{
		"title":    "Distributed systems",
		"abstract": "Raft consensus protocol",
	})
	index("doc-2", map[string]string{
		"title":    "Distributed storage",
		"abstract": "Replication overview",
	})
	index("doc-3", map[string]string{
		"title":    "Distributed systems draft",
		"abstract": "Draft Raft consensus notes",
	})

	fmt.Printf("fields=%v\n", engine.Fields())

	res, err := engine.SearchFieldClauses(ctx, []fts.FieldQueryClause{
		fts.MustFieldQuery("title", "distributed"),
		fts.ShouldFieldQuery("abstract", "consensus"),
		fts.MustNotFieldQuery("abstract", "draft"),
	}, 10)
	if err != nil {
		panic(err)
	}

	fmt.Printf("results=%d\n", res.TotalResultsCount)
	for _, item := range res.Results {
		fmt.Printf("id=%s unique=%d total=%d\n", item.ID, item.UniqueMatches, item.TotalMatches)
	}
}
