package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/flat"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
	"github.com/dariasmyr/fts-engine/pkg/textproc"
)

func main() {
	ctx := context.Background()
	pipeline := textproc.ObservabilityPipeline()
	engine := fts.New(flat.New(), keygen.Word, fts.WithPipeline(pipeline))

	docs := []fts.Document{
		{ID: "event-1", Fields: map[string]fts.Field{fts.DefaultField: {Value: "checkout-api/v2 failed with io.EOF from 10.0.0.1"}}},
		{ID: "event-2", Fields: map[string]fts.Field{fts.DefaultField: {Value: "payments-api/v1 returned timeout from 10.0.0.2"}}},
	}
	for _, doc := range docs {
		if err := engine.Index(ctx, doc); err != nil {
			panic(err)
		}
	}

	res, err := engine.SearchPlainText(ctx, "io.EOF", 10)
	if err != nil {
		panic(err)
	}
	for _, item := range res.Results {
		fmt.Println(item.ID)
	}
}
