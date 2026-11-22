// Package ollama implements a model-bound embedding.Client using Ollama's API.
// Client construction verifies the configured model tag's digest and model
// capabilities before embedding requests are accepted. Embed requests use that
// configured name or tag because Ollama does not accept a bare digest as a model
// reference; the resolved digest remains the immutable Revision identity.
package ollama
