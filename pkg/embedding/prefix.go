package embedding

import "context"

// WithPrefix returns a client that prepends prefix to every text without
// mutating caller-owned input. A nil client remains nil.
func WithPrefix(client Client, prefix string) Client {
	if client == nil || prefix == "" {
		return client
	}
	return prefixedClient{client: client, prefix: prefix}
}

type prefixedClient struct {
	client Client
	prefix string
}

func (c prefixedClient) Model() Model { return c.client.Model() }

func (c prefixedClient) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	prefixed := make([]string, len(texts))
	for i, text := range texts {
		prefixed[i] = c.prefix + text
	}
	return c.client.Embed(ctx, prefixed)
}
