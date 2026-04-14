package main

import (
	"context"
	"os"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func main() {
	if err := ftsbuiltin.RegisterSnapshotCodecs(); err != nil {
		panic(err)
	}

	if err := os.MkdirAll("./data/segments", 0755); err != nil {
		panic(err)
	}

	idx, err := ftsbuiltin.BuildIndex("radix")
	if err != nil {
		panic(err)
	}

	flt, err := ftsbuiltin.BuildFilter("bloom", ftsbuiltin.FilterOptions{
		BloomExpectedItems: 1_000_000,
		BloomBitsPerItem:   10,
		BloomK:             7,
	})
	if err != nil {
		panic(err)
	}

	svc := fts.New(idx, keygen.Word, fts.WithFilter(flt))
	if err := svc.IndexDocument(context.Background(), "doc-1", "snapshot with bloom filter"); err != nil {
		panic(err)
	}

	index, searchFilter := svc.SnapshotComponents()

	indexFile, err := os.Create("./data/segments/default.index.fidx")
	if err != nil {
		panic(err)
	}
	defer indexFile.Close()

	filterFile, err := os.Create("./data/segments/default.filter.fidx")
	if err != nil {
		panic(err)
	}
	defer filterFile.Close()

	if err := fts.SaveIndexSnapshot(indexFile, "radix", index); err != nil {
		panic(err)
	}
	if err := fts.SaveFilterSnapshot(filterFile, "bloom", searchFilter); err != nil {
		panic(err)
	}
}
