// Package contextcheck provides bounded context cancellation checks for vector work loops.
package contextcheck

import "context"

const contextCheckInterval = 256

// PeriodicError checks ctx once per bounded group of work items.
func PeriodicError(ctx context.Context, iteration int) error {
	if (iteration+1)%contextCheckInterval == 0 {
		return ctx.Err()
	}
	return nil
}
