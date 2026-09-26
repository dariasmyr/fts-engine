package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/segment"
	"github.com/dariasmyr/fts-engine/pkg/segmentbundle"
	"github.com/dariasmyr/fts-engine/pkg/textproc"
)

func main() {
	if err := run(os.Stdout); err != nil {
		panic(err)
	}
}

func run(out io.Writer) error {
	ctx := context.Background()
	pipeline := textproc.ObservabilityPipeline()
	service := fts.New(slicedradix.New(), fts.WordKeys, fts.WithPipeline(pipeline))
	if err := service.Index(ctx, fts.Document{
		ID: "doc-1",
		Fields: map[string]fts.Field{
			fts.DefaultField: {Value: "stream bundle example"},
		},
	}); err != nil {
		return err
	}

	index, _ := service.SnapshotComponents()
	source, ok := index.(segment.Source)
	if !ok {
		return fmt.Errorf("index does not implement segment.Source")
	}
	descriptors, ok := service.AnalyzerDescriptors()
	if !ok {
		return fmt.Errorf("analyzer descriptor is unavailable")
	}
	keyGenerator := fts.NewKeyGeneratorDescriptor("word-keys", 1)

	var blob bytes.Buffer
	if err := segmentbundle.SaveBundle(
		&blob,
		source,
		service.SnapshotCollectionStats(),
		service.SnapshotRegistry(),
		service.SnapshotTombstones(),
		segmentbundle.BundleSaveOptions{Analyzers: descriptors, KeyGenerator: &keyGenerator},
	); err != nil {
		return err
	}

	bundle, err := segmentbundle.LoadBundle(bytes.NewReader(blob.Bytes()))
	if err != nil {
		return err
	}
	restored, err := segmentbundle.RestoreService(bundle, fts.WordKeys, fts.WithPipeline(pipeline))
	if err != nil {
		return err
	}

	result, err := restored.SearchDocuments(ctx, "bundle", 10)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "bundle_bytes=%d results=%d\n", blob.Len(), result.TotalResultsCount)
	return err
}
