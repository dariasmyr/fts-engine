package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
	"github.com/dariasmyr/fts-engine/pkg/segment"
)

func main() {
	if err := ftsbuiltin.RegisterSnapshotCodecs(); err != nil {
		panic(err)
	}

	bundleFile, err := os.Open("./data/segments/default.bundle.fidx")
	if err != nil {
		panic(err)
	}
	defer bundleFile.Close()

	filterFile, err := os.Open("./data/segments/default.filter.fidx")
	if err != nil {
		panic(err)
	}
	defer filterFile.Close()

	loadedBundle, err := segment.LoadBundle(bundleFile)
	if err != nil {
		panic(err)
	}

	loadedFilter, err := fts.LoadFilterSnapshot(filterFile)
	if err != nil {
		panic(err)
	}

	restored, err := segment.RestoreService(loadedBundle, keygen.Word, fts.WithFilter(loadedFilter.Filter), fts.WithScorer(fts.BM25()))
	if err != nil {
		panic(err)
	}
	res, err := restored.SearchDocuments(context.Background(), "snapshot", 10)
	if err != nil {
		panic(err)
	}

	fmt.Println(res.TotalResultsCount)
}
