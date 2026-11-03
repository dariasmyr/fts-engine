package fts

import (
	"context"
	"strings"
	"testing"
)

type searchOnlyIndex struct{}

func (searchOnlyIndex) Search(string) ([]Posting, error) { return nil, nil }

func TestServiceRejectsIndexingIntoReadOnlyIndex(t *testing.T) {
	service := New(searchOnlyIndex{}, WordKeys)
	err := service.Index(context.Background(), Document{
		ID: "doc-1",
		Fields: map[string]Field{
			DefaultField: {Value: "alpha"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "index is read-only") {
		t.Fatalf("Index() error = %v, want read-only error", err)
	}
}
