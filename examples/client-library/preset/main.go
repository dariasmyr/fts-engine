package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftspreset"
	"github.com/dariasmyr/fts-engine/pkg/index/radix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func main() {
	engine := fts.New(
		radix.New(),
		keygen.Word,
		ftspreset.Multilingual(),
	)

	_ = engine.IndexDocument(context.Background(), "doc-1", "Hotels in France and отели в России")
	_ = engine.IndexDocument(context.Background(), "doc-2", "Hotel market overview")

	res, err := engine.SearchDocuments(context.Background(), "отели hotel", 10)
	if err != nil {
		panic(err)
	}

	fmt.Printf("results=%d\n", res.TotalResultsCount)
	for _, item := range res.Results {
		fmt.Printf("id=%s unique=%d total=%d\n", item.ID, item.UniqueMatches, item.TotalMatches)
	}
}
