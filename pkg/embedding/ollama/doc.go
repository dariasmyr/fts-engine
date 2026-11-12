// Package ollama implements a model-bound embedding.Client using Ollama's API.
// Client construction verifies the configured digest and model capabilities
// before embedding requests are accepted. Embed requests use the resolved digest;
// the configured name or tag remains only the model's logical ID.
package ollama
