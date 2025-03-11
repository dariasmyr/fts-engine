package tests

import (
	"context"
	"fts-hw/internal/storage/leveldb"
	"testing"
)

func TestIndex(t *testing.T) {
	storage, err := leveldb.NewStorage("./storage/leveldb.db")
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	defer storage.Close()

	content := "This is a test document for indexing."
	words := []string{"test", "document", "indexing"}

	docID, err := storage.SaveDocumentWithIndexing(context.Background(), []byte(content), words, "123")
	if err != nil {
		t.Fatalf("Failed to add document: %v", err)
	}

	if docID == "" {
		t.Fatalf("Invalid document ID returned")
	}

	t.Logf("Successfully added document with ID %s", docID)
}
