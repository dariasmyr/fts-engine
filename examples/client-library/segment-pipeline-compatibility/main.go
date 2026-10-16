package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/v2/pkg/ftspersist"
	"github.com/dariasmyr/fts-engine/v2/pkg/index/flat"
	"github.com/dariasmyr/fts-engine/v2/pkg/textproc"
)

func main() {
	registry, err := ftsbuiltin.NewSnapshotRegistry()
	if err != nil {
		panic(err)
	}
	dir, err := os.MkdirTemp("", "fts-observability-segment-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()
	pipeline := textproc.ObservabilityPipeline()
	engine := fts.New(flat.New(), fts.WordKeys, fts.WithPipeline(pipeline))
	if err := engine.Index(ctx, fts.Document{
		ID: "event-1",
		Fields: map[string]fts.Field{
			fts.DefaultField: {Value: "checkout-api/v2 failed with io.EOF"},
		},
	}); err != nil {
		panic(err)
	}

	paths := ftspersist.SegmentPaths{Dir: dir}
	if err := ftspersist.SaveSegment(ctx, paths, engine, "", ftspersist.SaveOptions{Registry: registry}); err != nil {
		panic(err)
	}

	descriptor := pipeline.Descriptor()
	loaded, err := ftspersist.LoadSegment(
		paths,
		fts.WordKeys,
		ftspersist.SegmentLoadOptions{Registry: registry, Access: ftspersist.AccessFile},
		fts.WithPipeline(pipeline),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := loaded.Close(); err != nil {
			panic(err)
		}
	}()

	res, err := loaded.Service.SearchQuery(ctx, fts.TermQuery{Term: "io.EOF"}, fts.SearchOptions{MaxResults: 10})
	if err != nil {
		panic(err)
	}
	fmt.Printf("pipeline=%s fingerprint=%s results=%d\n", descriptor.Name, descriptor.Fingerprint, res.TotalResultsCount)
}
