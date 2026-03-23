package main

import (
	"bytes"
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func main() {
	idx, err := ftsbuiltin.BuildIndex("radix")
	if err != nil {
		panic(err)
	}

	service := fts.New(idx, keygen.Word)
	_ = service.IndexDocument(context.Background(), "doc-1", "snapshot demo text")

	var buf bytes.Buffer
	if err := ftsbuiltin.SaveServiceSnapshot(&buf, service, "radix", ""); err != nil {
		panic(err)
	}

	loaded, err := ftsbuiltin.LoadSegmentSnapshot(bytes.NewReader(buf.Bytes()))
	if err != nil {
		panic(err)
	}
	restored := fts.New(loaded.Index, keygen.Word)

	res, err := restored.SearchDocuments(context.Background(), "snapshot", 10)
	if err != nil {
		panic(err)
	}

	fmt.Printf("restored results=%d\n", res.TotalResultsCount)
}
