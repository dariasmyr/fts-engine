package fts_test

import (
	"context"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/hamt"
	"github.com/dariasmyr/fts-engine/pkg/index/hamtpointered"
	"github.com/dariasmyr/fts-engine/pkg/index/radix"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
)

func TestBuiltInIndexesSupportPrefixSearch(t *testing.T) {
	tests := []struct {
		name  string
		index fts.Index
	}{
		{name: "radix", index: radix.New()},
		{name: "slicedradix", index: slicedradix.New()},
		{name: "hamt", index: hamt.New()},
		{name: "hamtpointered", index: hamtpointered.New()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, ok := tt.index.(fts.PrefixIndex); !ok {
				t.Fatalf("index %q does not implement PrefixIndex", tt.name)
			}

			svc := fts.New(tt.index, fts.WordKeys)
			ctx := context.Background()
			if err := svc.IndexDocument(ctx, "doc-a", "bar barge"); err != nil {
				t.Fatalf("IndexDocument(doc-a) error = %v", err)
			}
			if err := svc.IndexDocument(ctx, "doc-b", "bar"); err != nil {
				t.Fatalf("IndexDocument(doc-b) error = %v", err)
			}
			if err := svc.IndexDocument(ctx, "doc-c", "hotel"); err != nil {
				t.Fatalf("IndexDocument(doc-c) error = %v", err)
			}

			res, err := svc.SearchDocuments(ctx, "bar*", 10)
			if err != nil {
				t.Fatalf("SearchDocuments() error = %v", err)
			}
			if len(res.Results) != 2 {
				t.Fatalf("expected 2 prefix hits, got %+v", res.Results)
			}
			if got := res.Results[0]; got.ID != "doc-a" || got.TotalMatches != 2 {
				t.Fatalf("first result = %+v, want doc-a with TotalMatches=2", got)
			}
			if got := res.Results[1]; got.ID != "doc-b" || got.TotalMatches != 1 {
				t.Fatalf("second result = %+v, want doc-b with TotalMatches=1", got)
			}
		})
	}
}
