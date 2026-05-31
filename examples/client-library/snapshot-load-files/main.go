package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/ftspersist"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func main() {
	if err := ftsbuiltin.RegisterSnapshotCodecs(); err != nil {
		panic(err)
	}

	loaded, err := ftspersist.LoadSnapshot(ftspersist.SnapshotPaths{
		IndexPath:  "./data/segments/default.index.fidx",
		FilterPath: "./data/segments/default.filter.fidx",
	}, keygen.Word, fts.WithScorer(fts.BM25()))
	if err != nil {
		panic(err)
	}
	defer loaded.Close()

	restored := loaded.Service

	// Snapshot restore stays writable, so we can keep indexing after load.
	if err := restored.IndexDocument(context.Background(), "doc-2", "restored snapshot stays writable"); err != nil {
		panic(err)
	}

	res, err := restored.SearchDocuments(context.Background(), "snapshot", 10)
	if err != nil {
		panic(err)
	}

	fmt.Println(res.TotalResultsCount)
}
