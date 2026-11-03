package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/v2/pkg/ftspersist"
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

	res, err := loaded.Service.SearchText(context.Background(), "snapshot", fts.SearchOptions{MaxResults: 10})
	if err != nil {
		panic(err)
	}

	fmt.Println(res.TotalResultsCount)
}
