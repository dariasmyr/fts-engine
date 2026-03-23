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

	for _, filterName := range []string{"bloom", "cuckoo"} {
		idx, err := ftsbuiltin.BuildIndex("radix")
		if err != nil {
			panic(err)
		}

		opts := ftsbuiltin.FilterOptions{
			BloomExpectedItems: 100_000,
			BloomBitsPerItem:   10,
			BloomK:             7,
			CuckooBucketCount:  1 << 15,
			CuckooBucketSize:   4,
			CuckooMaxKicks:     500,
		}

		flt, err := ftsbuiltin.BuildFilter(filterName, opts)
		if err != nil {
			panic(err)
		}

		svc := fts.New(idx, keygen.Word, fts.WithFilter(flt))

		_ = svc.IndexDocument(ctx, "doc-1", "hotels in france")
		_ = svc.IndexDocument(ctx, "doc-2", "hotel market overview")

		res, err := svc.SearchDocuments(ctx, "hotel france", 10)
		if err != nil {
			panic(err)
		}

		fmt.Printf("filter=%s results=%d\n", filterName, res.TotalResultsCount)
	}
}
