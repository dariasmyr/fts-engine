package tests

import (
	"fts-hw/internal/services/loader"
	"log/slog"
	"os"
	"testing"
)

func TestLoader(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	loader := loader.NewLoader(log, "./data/enwiki-latest-abstract1.xml.gz")

	docs, err := loader.LoadDocuments()
	if err != nil {
		t.Fatalf("Failed to load documents: %v", err)
	}

	if len(docs) == 0 {
		t.Fatalf("Expected documents, but got 0")
	}

	t.Logf("Successfully loaded %d documents", len(docs))
}
