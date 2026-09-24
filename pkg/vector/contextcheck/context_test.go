package contextcheck

import (
	"context"
	"errors"
	"testing"
)

func TestPeriodicErrorChecksAtIntervalBoundary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for iteration := 0; iteration < contextCheckInterval-1; iteration++ {
		if err := PeriodicError(ctx, iteration); err != nil {
			t.Fatalf("iteration %d error = %v", iteration, err)
		}
	}
	if err := PeriodicError(ctx, contextCheckInterval-1); !errors.Is(err, context.Canceled) {
		t.Fatalf("boundary error = %v, want context cancellation", err)
	}
	if err := PeriodicError(context.Background(), contextCheckInterval-1); err != nil {
		t.Fatalf("active context boundary error = %v", err)
	}
}
