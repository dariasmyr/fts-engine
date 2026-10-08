package semanticformat

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dariasmyr/fts-engine/internal/memorystore"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestDecodeVectorFileCancellation(t *testing.T) {
	data, limits := testVectorFile(t, 1024, 64)

	t.Run("nil context", func(t *testing.T) {
		if _, _, err := DecodeVectorFile(nil, data, limits); !errors.Is(err, vector.ErrNilContext) {
			t.Fatalf("DecodeVectorFile error = %v, want vector.ErrNilContext", err)
		}
	})

	t.Run("pre-canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, _, err := DecodeVectorFile(ctx, data, limits); !errors.Is(err, context.Canceled) {
			t.Fatalf("DecodeVectorFile error = %v, want context.Canceled", err)
		}
	})

	t.Run("during component decode", func(t *testing.T) {
		ctx := newCancelAfterChecksContext(8)
		if _, _, err := DecodeVectorFile(ctx, data, limits); !errors.Is(err, context.Canceled) {
			t.Fatalf("DecodeVectorFile error = %v, want context.Canceled", err)
		}
	})
}

func testVectorFile(t *testing.T, rows, dimensions int) ([]byte, VectorLimits) {
	t.Helper()
	calculator, err := vector.NewCalculator(dimensions, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	store, err := memorystore.NewPrepared(calculator, make([]float32, rows*dimensions))
	if err != nil {
		t.Fatal(err)
	}
	limits := VectorLimits{
		MaxDimensions: dimensions,
		MaxVectors: rows,
		MaxVectorBytes: uint64(rows * dimensions * float32ByteSize),
		MaxK: rows,
	}
	var output bytes.Buffer
	if _, err := WriteVectorFile(context.Background(), &output, store, rows, limits); err != nil {
		t.Fatal(err)
	}
	return output.Bytes(), limits
}

type cancelAfterChecksContext struct {
	remaining int
	done      chan struct{}
	canceled  bool
}

func newCancelAfterChecksContext(checks int) *cancelAfterChecksContext {
	return &cancelAfterChecksContext{remaining: checks, done: make(chan struct{})}
}

func (c *cancelAfterChecksContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *cancelAfterChecksContext) Done() <-chan struct{}       { return c.done }
func (c *cancelAfterChecksContext) Value(any) any                { return nil }

func (c *cancelAfterChecksContext) Err() error {
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
