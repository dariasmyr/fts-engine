package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func main() {
	if err := ftsbuiltin.RegisterSnapshotCodecs(); err != nil {
		panic(err)
	}

	indexFile, err := os.Open("./data/segments/default.index.fidx")
	if err != nil {
		panic(err)
	}
	defer indexFile.Close()

	filterFile, err := os.Open("./data/segments/default.filter.fidx")
	if err != nil {
		panic(err)
	}
	defer filterFile.Close()

	loadedIndex, err := fts.LoadIndexSnapshot(indexFile)
	if err != nil {
		panic(err)
	}

	loadedFilter, err := fts.LoadFilterSnapshot(filterFile)
	if err != nil {
		panic(err)
	}

	restored := fts.New(loadedIndex.Index, keygen.Word, fts.WithFilter(loadedFilter.Filter))
	res, err := restored.SearchDocuments(context.Background(), "snapshot", 10)
	if err != nil {
		panic(err)
	}

	fmt.Println(res.TotalResultsCount)
}
