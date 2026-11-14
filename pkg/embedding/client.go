package embedding

import (
	"context"
	"fmt"
	"math"
)

// Client converts a batch of texts into vectors in the same order. A client is
// permanently bound to the immutable Model returned by Model. Clients must
// support concurrent calls and must not retain or mutate the input slice.
type Client interface {
	Model() Model
	Embed(context.Context, []string) ([][]float32, error)
}

// ClientFunc is the embedding function accepted by NewClient.
type ClientFunc func(context.Context, []string) ([][]float32, error)

// NewClient creates a model-bound Client from a custom embedding function. It
// is intended for custom providers and locally hosted embedding models.
func NewClient(model Model, embed ClientFunc) (Client, error) {
	if err := model.Validate(); err != nil {
		return nil, err
	}
	if embed == nil {
		return nil, ErrNilClient
	}
	return boundClient{model: model, embed: embed}, nil
}

type boundClient struct {
	model Model
	embed ClientFunc
}

func (c boundClient) Model() Model { return c.model }

func (c boundClient) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	vectors, err := c.embed(ctx, texts)
	if err != nil {
		return nil, err
	}
	for row, values := range vectors {
		if len(values) != c.model.Dimensions {
			return nil, fmt.Errorf("%w: vector %d has %d dimensions, want %d", ErrInvalidResult, row, len(values), c.model.Dimensions)
		}
		for column, value := range values {
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return nil, fmt.Errorf("%w: vector %d contains non-finite value at dimension %d", ErrInvalidResult, row, column)
			}
		}
	}
	return vectors, nil
}
