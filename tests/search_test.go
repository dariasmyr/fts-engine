package tests

import (
	"context"
	"fts-hw/internal/services/fts"
	"fts-hw/internal/storage/leveldb"
	"log/slog"
	"os"
	"testing"
)

func TestSearch(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	storage, err := leveldb.NewStorage("./storage/leveldb.db")
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	defer storage.Close()

	searchEngine := fts.New(log, storage, storage)

	extract := "Search engine test document."
	content := []byte("Search engine test document.")
	_, err = searchEngine.ProcessDocument(context.Background(), extract, content, nil)
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
