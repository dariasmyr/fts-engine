package main

import (
	"context"
	"os"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/ftspersist"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func main() {
	if err := ftsbuiltin.RegisterSnapshotCodecs(); err != nil {
		panic(err)
	}

	if err := os.MkdirAll("./data/segments", 0o755); err != nil {
		panic(err)
	}

	idx, err := ftsbuiltin.BuildIndex("slicedradix")
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

	svc := fts.New(idx, keygen.Word, fts.WithFilter(flt), fts.WithScorer(fts.BM25()))
	if err := svc.IndexDocument(context.Background(), "doc-1", "snapshot with bloom filter"); err != nil {
		panic(err)
	}

	if err := ftspersist.SaveSnapshot(ftspersist.SnapshotPaths{
		IndexPath:  "./data/segments/default.index.fidx",
		FilterPath: "./data/segments/default.filter.fidx",
	}, svc, "slicedradix", "bloom", ftspersist.SaveOptions{SyncFile: true}); err != nil {
		panic(err)
	}
}
