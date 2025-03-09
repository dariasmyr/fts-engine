package tests

import (
	"context"
	"fts-hw/internal/storage/leveldb"
	"testing"
)

func TestStats(t *testing.T) {
	storage, err := leveldb.NewStorage("./storage/leveldb.db")
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	defer storage.Close()

	stats, err := storage.GetDatabaseStats(context.Background())
	if err != nil {
		t.Fatalf("Failed to get database stats: %v", err)
	}
	t.Logf("Stats: %s", stats)
}
