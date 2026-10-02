package semanticpersist

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkPublish(b *testing.B) {
	service, encoder := persistenceService(b)
	addAndFlush(b, service, encoder, "doc-a")
	root := b.TempDir()
	options := Options{Durability: DurabilityAsynchronous}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		path := fmt.Sprintf("%s/store-%d", root, i)
		if _, err := Publish(context.Background(), path, service, options); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpen(b *testing.B) {
	service, encoder := persistenceService(b)
	addAndFlush(b, service, encoder, "doc-a")
	root := b.TempDir()
	if _, err := Publish(context.Background(), root, service, Options{Durability: DurabilityAsynchronous}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		store, err := Open(context.Background(), root, OpenOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if err := store.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
