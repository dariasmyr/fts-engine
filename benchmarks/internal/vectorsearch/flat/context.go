package flat

import "context"

const contextCheckInterval = 256

func periodicContextError(ctx context.Context, iteration int) error {
	if (iteration+1)%contextCheckInterval == 0 {
		return ctx.Err()
	}
	return nil
}
