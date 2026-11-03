package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/index/slicedradix"
)

func main() {
	if err := run(os.Stdout); err != nil {
		panic(err)
	}
}

func run(out io.Writer) error {
	ctx := context.Background()
	engine := fts.New(slicedradix.New(), fts.WordKeys)
	for _, document := range []fts.Document{
		{ID: "event-1", Fields: map[string]fts.Field{fts.DefaultField: {Value: "checkout timeout"}}},
		{ID: "event-2", Fields: map[string]fts.Field{fts.DefaultField: {Value: "checkout complete"}}},
		{ID: "event-3", Fields: map[string]fts.Field{fts.DefaultField: {Value: "billing timeout"}}},
	} {
		if err := engine.Index(ctx, document); err != nil {
			return err
		}
	}

	result, err := engine.SearchText(ctx, "timeout", fts.SearchOptions{
		CandidateIDs: []fts.DocID{"event-2", "event-3", "missing"},
		MaxResults:   10,
	})
	if err != nil {
		return err
	}
	for _, match := range result.Results {
		if _, err := fmt.Fprintln(out, match.ID); err != nil {
			return err
		}
	}
	return nil
}
