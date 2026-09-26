package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/ftspersist"
)

func main() {
	registry, err := ftsbuiltin.NewSnapshotRegistry()
	if err != nil {
		panic(err)
	}

	loaded, err := ftspersist.LoadSegment(ftspersist.SegmentPaths{Dir: "./data/segments/default"}, fts.WordKeys, ftspersist.SegmentLoadOptions{Access: ftspersist.AccessFile, Registry: registry}, fts.WithScorer(fts.BM25()))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := loaded.Close(); err != nil {
			panic(err)
		}
	}()

	res, err := loaded.Service.SearchDocuments(context.Background(), "snapshot", 10)
	if err != nil {
		panic(err)
	}

	fmt.Println(res.TotalResultsCount)
}
