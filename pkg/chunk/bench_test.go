package chunk

import (
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func BenchmarkSplitter(b *testing.B) {
	paragraph := strings.Repeat("semantic search paragraph ", 20) + "\n\n"
	text := strings.Repeat(paragraph, 2_000)
	splitter, err := NewSplitter(Descriptor{
		ID:           "benchmark-v1",
		TargetBytes:  512,
		MaxBytes:     768,
		OverlapBytes: 64,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(text)))
	b.ResetTimer()
	for b.Loop() {
		if _, err := splitter.Split("doc", fts.DefaultField, text); err != nil {
			b.Fatal(err)
		}
	}
}
