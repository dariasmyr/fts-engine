package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftspersist"
	"github.com/dariasmyr/fts-engine/pkg/index/flat"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
	"github.com/dariasmyr/fts-engine/pkg/textproc"
)

func main() {
	dir, err := os.MkdirTemp("", "fts-observability-segment-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()
	pipeline := textproc.ObservabilityPipeline()
	engine := fts.New(flat.New(), keygen.Word, fts.WithPipeline(pipeline))
	if err := engine.Index(ctx, fts.Document{
		ID: "event-1",
		Fields: map[string]fts.Field{
			fts.DefaultField: {Value: "checkout-api/v2 failed with io.EOF"},
		},
	}); err != nil {
		panic(err)
	}

	paths := ftspersist.SegmentPaths{Dir: dir}
	if err := ftspersist.SaveSegment(paths, engine, "", ftspersist.SaveOptions{}); err != nil {
		panic(err)
	}

	descriptor := pipeline.Descriptor()
	loaded, err := ftspersist.LoadSegment(
		paths,
		keygen.Word,
		ftspersist.SegmentLoadOptions{
			Access:                      ftspersist.AccessFile,
			ExpectedAnalyzerFingerprint: descriptor.Fingerprint,
		},
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

	res, err := loaded.Service.SearchPlainText(ctx, "io.EOF", 10)
	if err != nil {
		panic(err)
	}
	fmt.Printf("analyzer=%s@%d results=%d\n", descriptor.Name, descriptor.Version, res.TotalResultsCount)
}
