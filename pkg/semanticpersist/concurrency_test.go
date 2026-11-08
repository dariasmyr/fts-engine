package semanticpersist

import (
	"context"
	"fmt"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
)

func TestConcurrentSearchDuringSave(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	addTestSegment(t, ctx, index)

	generation, err := Save(ctx, root, index, SaveOptions{
		ExpectedGeneration: &Generation{},
	})
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	searchDone := make(chan error, 1)
	go func() {
		<-start
		query := []semantic.ChunkEmbedding{{Embedding: []float32{1, 2}}}
		for range 500 {
			result, err := index.Search(ctx, query, 1, semantic.SearchOptions{})
			if err != nil {
				searchDone <- err
				return
			}
			if len(result.Hits) != 1 || result.Hits[0].DocID != "doc" {
				searchDone <- fmt.Errorf("search hits = %+v, want doc", result.Hits)
				return
			}
		}
		searchDone <- nil
	}()

	close(start)
	for range 3 {
		generation, err = Save(ctx, root, index, SaveOptions{
			ExpectedGeneration: &generation,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := <-searchDone; err != nil {
		t.Fatal(err)
	}
}
