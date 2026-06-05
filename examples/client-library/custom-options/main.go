package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/filter"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
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
		slicedradix.New(),
		keygen.Word,
		fts.WithPipeline(pipe),
		fts.WithFilter(bloom),
	)

	_ = engine.Index(context.Background(), fts.Document{ID: "doc-1", Fields: map[string]fts.Field{fts.DefaultField: {Value: "Search with custom index"}}})
	_ = engine.Index(context.Background(), fts.Document{ID: "doc-2", Fields: map[string]fts.Field{fts.DefaultField: {Value: "Another searchable document"}}})

	res, err := engine.SearchDocuments(context.Background(), "searchable", 5)
	if err != nil {
		panic(err)
	}

	fmt.Printf("results=%d\n", res.TotalResultsCount)
	for _, item := range res.Results {
		fmt.Printf("id=%s unique=%d total=%d\n", item.ID, item.UniqueMatches, item.TotalMatches)
	}
}
