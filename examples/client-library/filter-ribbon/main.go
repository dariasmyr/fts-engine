package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func main() {
	ctx := context.Background()

	idx, err := ftsbuiltin.BuildIndex("radix")
	if err != nil {
		panic(err)
	}

	opts := ftsbuiltin.FilterOptions{
		RibbonExpectedItems: 100_000,
		RibbonExtraCells:    25_000,
		RibbonWindowSize:    24,
		RibbonSeed:          0,
		RibbonMaxAttempts:   5,
	}

	flt, err := ftsbuiltin.BuildFilter("ribbon", opts)
	if err != nil {
		panic(err)
	}

	svc := fts.New(idx, keygen.Word, fts.WithFilter(flt))

	_ = svc.IndexDocument(ctx, "doc-1", "hotels in france")
	_ = svc.IndexDocument(ctx, "doc-2", "hotel market overview")

	if err = svc.BuildFilter(); err != nil {
		panic(err)
	}

	res, err := svc.SearchDocuments(ctx, "hotel france", 10)
	if err != nil {
		panic(err)
	}

	fmt.Printf("filter=ribbon results=%d\n", res.TotalResultsCount)
}
