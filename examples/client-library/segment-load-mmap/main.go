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

	loaded, err := ftspersist.LoadSegment(
		ftspersist.SegmentPaths{Dir: "./data/segments/default"},
		keygen.Word,
		ftspersist.SegmentLoadOptions{Access: ftspersist.AccessMmap},
		fts.WithScorer(fts.BM25()),
	)
	if err != nil {
		panic(err)
	}
	defer loaded.Close()

	res, err := loaded.Service.SearchDocuments(context.Background(), "snapshot", 10)
	if err != nil {
		panic(err)
	}

	fmt.Println(res.TotalResultsCount)
}
