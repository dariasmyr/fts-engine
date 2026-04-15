package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/filter"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/radix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
	"github.com/dariasmyr/fts-engine/pkg/textproc"
)

func main() {
	pipe := textproc.NewPipeline(
		textproc.AlnumTokenizer{},
		textproc.LowercaseFilter{},
		textproc.MinLengthOrNumericFilter{MinLength: 2},
	)

	bloom := filter.NewBloomFilter(100_000, 10, 7)

	engine := fts.New(
		radix.New(),
		keygen.Word,
		fts.WithPipeline(pipe),
		fts.WithFilter(bloom),
	)

	_ = engine.IndexDocument(context.Background(), "doc-1", "Search with custom index")
	_ = engine.IndexDocument(context.Background(), "doc-2", "Another searchable document")

	res, err := engine.SearchDocuments(context.Background(), "searchable", 5)
	if err != nil {
		panic(err)
	}

	fmt.Printf("results=%d\n", res.TotalResultsCount)
	for _, item := range res.Results {
		fmt.Printf("id=%s unique=%d total=%d\n", item.ID, item.UniqueMatches, item.TotalMatches)
	}
}
