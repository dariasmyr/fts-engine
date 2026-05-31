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

	loaded, err := ftspersist.LoadSegmentData(
		ftspersist.SegmentPaths{Dir: "./data/segments/default"},
		ftspersist.SegmentLoadOptions{Access: ftspersist.AccessFile},
	)
	if err != nil {
		panic(err)
	}
	defer loaded.Close()

	// Low-level restore keeps full control over optional restore options.
	restored, err := ftspersist.RestoreSegmentService(
		loaded,
		keygen.Word,
		fts.WithScorer(fts.BM25()),
	)
	if err != nil {
		panic(err)
	}

	res, err := restored.SearchDocuments(context.Background(), "snapshot", 10)
	if err != nil {
		panic(err)
	}

	fmt.Println(res.TotalResultsCount)
}
