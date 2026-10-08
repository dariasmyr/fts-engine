package semantic

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestGroupDocumentHitsCancellation(t *testing.T) {
	candidates := make([]ChunkHit, 600)
	for i := range candidates {
		candidates[i] = ChunkHit{
			Ref: chunk.Ref{DocID: fts.DocID("document"), ID: chunk.ID(fmt.Sprintf("chunk-%d", i))},
			Distance: float64(i),
		}
	}

	t.Run("nil context", func(t *testing.T) {
		if _, _, err := groupDocumentHits(nil, candidates, 1, len(candidates)); !errors.Is(err, vector.ErrNilContext) {
			t.Fatalf("groupDocumentHits error = %v, want vector.ErrNilContext", err)
		}
	})

	t.Run("pre-canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, _, err := groupDocumentHits(ctx, candidates, 1, len(candidates)); !errors.Is(err, context.Canceled) {
			t.Fatalf("groupDocumentHits error = %v, want context.Canceled", err)
		}
	})

	t.Run("during merge", func(t *testing.T) {
		ctx := newGroupingCancelAfterChecksContext(3)
		if _, _, err := groupDocumentHits(ctx, candidates, 1, len(candidates)); !errors.Is(err, context.Canceled) {
			t.Fatalf("groupDocumentHits error = %v, want context.Canceled", err)
		}
	})
}

type groupingCancelAfterChecksContext struct {
	remaining int
	done      chan struct{}
	canceled  bool
}

func newGroupingCancelAfterChecksContext(checks int) *groupingCancelAfterChecksContext {
	return &groupingCancelAfterChecksContext{remaining: checks, done: make(chan struct{})}
}

func (c *groupingCancelAfterChecksContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *groupingCancelAfterChecksContext) Done() <-chan struct{}       { return c.done }
func (c *groupingCancelAfterChecksContext) Value(any) any                { return nil }

func (c *groupingCancelAfterChecksContext) Err() error {
	if c.canceled {
		return context.Canceled
	}
	c.remaining--
	if c.remaining == 0 {
		c.canceled = true
		close(c.done)
		return context.Canceled
	}
	return nil
}
