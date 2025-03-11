package tests

import (
	"context"
	"fts-hw/internal/domain/models"
	"fts-hw/internal/services/fts"
	"fts-hw/internal/storage/leveldb"
	"log/slog"
	"os"
	"testing"
)

func TestSearch(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	storage, err := leveldb.NewStorage(".././storage/fts-test-queue-dump.db")
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	defer storage.Close()

	searchEngine := fts.New(log, storage, storage)

	document := models.Document{
		DocumentBase: models.DocumentBase{
			Title:    "Test Document",
			URL:      "https://example.com",
			Abstract: "This is a test document.",
		},
		Extract: "This is a test document.",
		ID:      "1",
	}
	_, err = searchEngine.ProcessDocument(context.Background(), document, nil)
	if err != nil {
		t.Fatalf("Failed to add document: %v", err)
	}

	searchResults, err := searchEngine.Search(context.Background(), "test", 10)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(searchResults.ResultDocs) == 0 {
		t.Fatalf("Expected search results, but got 0")
	}

	t.Logf("Search returned %d results", len(searchResults.ResultDocs))
	for _, result := range searchResults.ResultDocs {
		t.Log(result)
	}
}
