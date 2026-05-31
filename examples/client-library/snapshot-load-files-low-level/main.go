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

	loaded, err := ftspersist.LoadSnapshotData(ftspersist.SnapshotPaths{
		IndexPath:  "./data/segments/default.index.fidx",
		FilterPath: "./data/segments/default.filter.fidx",
	})
	if err != nil {
		panic(err)
	}
	defer loaded.Close()

	// Low-level restore keeps full control over optional restore options.
	restored := fts.New(
		loaded.Index,
		keygen.Word,
		fts.WithFilter(loaded.Filter),
		fts.WithScorer(fts.BM25()),
		fts.WithCollectionStatsSnapshot(loaded.CollectionStats),
		fts.WithDocRegistrySnapshot(loaded.Registry),
		fts.WithTombstonesSnapshot(loaded.Tombstones),
	)

	if err := restored.IndexDocument(context.Background(), "doc-2", "restored snapshot stays writable"); err != nil {
		panic(err)
	}

	res, err := restored.SearchDocuments(context.Background(), "snapshot", 10)
	if err != nil {
		panic(err)
	}

	fmt.Println(res.TotalResultsCount)
}
