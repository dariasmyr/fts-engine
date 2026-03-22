package main

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/radix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func main() {
	if err := registerRadixSnapshotCodec(); err != nil {
		panic(err)
	}

	service := fts.New(radix.New(), keygen.Word)
	_ = service.IndexDocument(context.Background(), "doc-1", "snapshot demo text")

	var buf bytes.Buffer
	if err := service.SaveSnapshot(&buf, "radix", ""); err != nil {
		panic(err)
	}

	restored, err := fts.NewFromSnapshot(bytes.NewReader(buf.Bytes()), keygen.Word)
	if err != nil {
		panic(err)
	}

	res, err := restored.SearchDocuments(context.Background(), "snapshot", 10)
	if err != nil {
		panic(err)
	}

	fmt.Printf("restored results=%d\n", res.TotalResultsCount)
}

func registerRadixSnapshotCodec() error {
	return fts.RegisterIndexSnapshotCodec(
		"radix",
		func(index fts.Index, w io.Writer) error {
			serializable, ok := index.(fts.Serializable)
			if !ok {
				return fmt.Errorf("index radix does not support serialization")
			}
			return serializable.Serialize(w)
		},
		radix.Load,
	)
}
