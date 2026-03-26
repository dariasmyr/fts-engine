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
	opts := ftsbuiltin.FilterOptions{
		BloomExpectedItems: 1_000_000,
		BloomBitsPerItem:   10,
		BloomK:             7,
	}

	idx, err := ftsbuiltin.BuildIndex("radix")
	if err != nil {
		panic(err)
	}
	flt, err := ftsbuiltin.BuildFilter("bloom", opts)
	if err != nil {
		panic(err)
	}

	svc := fts.New(idx, keygen.Word, fts.WithFilter(flt))
	if err := svc.IndexDocument(context.Background(), "doc-1", "snapshot with bloom filter"); err != nil {
		panic(err)
	}

	var buf bytes.Buffer
	if err := ftsbuiltin.SaveServiceSnapshot(&buf, svc, "radix", "bloom"); err != nil {
		panic(err)
	}

	loaded, err := ftsbuiltin.LoadSegmentSnapshot(bytes.NewReader(buf.Bytes()))
	if err != nil {
		panic(err)
	}

	restored := fts.New(loaded.Index, keygen.Word, fts.WithFilter(loaded.Filter))
	res, err := restored.SearchDocuments(context.Background(), "snapshot", 10)
	if err != nil {
		panic(err)
	}

	fmt.Println(res.TotalResultsCount)
}
