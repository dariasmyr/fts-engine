package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
)

const defaultMaxResponseBytes = 32 << 20

var ErrInvalidConfig = errors.New("embedding/ollama: invalid configuration")

var ErrModelMismatch = errors.New("embedding/ollama: model metadata mismatch")

type Config struct {
	BaseURL string
	// Model is the Ollama model name or tag. New resolves its immutable digest.
	Model string

	// Dimensions and ContextTokens are verified against Ollama model metadata.
	Dimensions    int
	ContextTokens int
	// DocumentPrefix and QueryPrefix become part of the resolved model contract.
	DocumentPrefix string
	QueryPrefix    string
	// Truncate permits Ollama to truncate inputs that exceed model limits.
	Truncate bool

	HTTPClient       *http.Client
	MaxResponseBytes int64
}

type Client struct {
	endpoint         string
	model            embedding.Model
	truncate         bool
	httpClient       *http.Client
	maxResponseBytes int64
}

var _ embedding.Client = (*Client)(nil)

func New(ctx context.Context, config Config) (*Client, error) {
	if ctx == nil {
		return nil, embedding.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config.BaseURL == "" || strings.TrimSpace(config.Model) == "" ||
		config.Dimensions <= 0 || config.ContextTokens <= 0 ||
		config.MaxResponseBytes < 0 || config.MaxResponseBytes == math.MaxInt64 {
		return nil, ErrInvalidConfig
	}
	baseURL, err := url.Parse(config.BaseURL)
	if err != nil || (baseURL.Scheme != "http" && baseURL.Scheme != "https") || baseURL.Host == "" {
		return nil, ErrInvalidConfig
	}
	if baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return nil, ErrInvalidConfig
	}
	baseURL.Path = strings.TrimRight(baseURL.Path, "/")
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	maxResponseBytes := config.MaxResponseBytes
	if maxResponseBytes == 0 {
		maxResponseBytes = defaultMaxResponseBytes
	}
	result := &Client{
		endpoint: baseURL.String() + "/api/embed",
		model: embedding.Model{
			ID:             config.Model,
			Dimensions:     config.Dimensions,
			ContextTokens:  config.ContextTokens,
			DocumentPrefix: config.DocumentPrefix,
			QueryPrefix:    config.QueryPrefix,
		},
		truncate:         config.Truncate,
		httpClient:       client,
		maxResponseBytes: maxResponseBytes,
	}
	if err := result.resolveAndValidateModel(ctx, baseURL.String()); err != nil {
		return nil, err
	}
	return result, nil
}

type APIError struct {
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("embedding/ollama: API returned status %d: %s", e.StatusCode, e.Body)
}

// Model returns the immutable model metadata verified when the client was
// created.
func (c *Client) Model() embedding.Model { return c.model }

func (c *Client) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if ctx == nil {
		return nil, embedding.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(texts) == 0 {
		return [][]float32{}, nil
	}
	payload, err := json.Marshal(struct {
		Model      string   `json:"model"`
		Input      []string `json:"input"`
		Truncate   bool     `json:"truncate"`
		Dimensions int      `json:"dimensions,omitempty"`
	}{Model: c.model.ID, Input: texts, Truncate: c.truncate, Dimensions: c.model.Dimensions})
	if err != nil {
		return nil, fmt.Errorf("embedding/ollama: encode request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("embedding/ollama: create request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("embedding/ollama: request: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, c.maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("embedding/ollama: read response: %w", err)
	}
	if int64(len(body)) > c.maxResponseBytes {
		return nil, errors.New("embedding/ollama: response exceeds configured limit")
	}
	if response.StatusCode != http.StatusOK {
		return nil, &APIError{StatusCode: response.StatusCode, Body: strings.TrimSpace(string(body))}
	}
	var decoded struct {
		Embeddings [][]float32 `json:"embeddings"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("embedding/ollama: decode response: %w", err)
	}
	if len(decoded.Embeddings) != len(texts) {
		return nil, fmt.Errorf("embedding/ollama: returned %d embeddings for %d inputs", len(decoded.Embeddings), len(texts))
	}
	for row, vector := range decoded.Embeddings {
		if len(vector) != c.model.Dimensions {
			return nil, fmt.Errorf("embedding/ollama: embedding %d has %d dimensions, want %d", row, len(vector), c.model.Dimensions)
		}
		for column, value := range vector {
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return nil, fmt.Errorf("embedding/ollama: embedding %d contains non-finite value at dimension %d", row, column)
			}
		}
	}
	return decoded.Embeddings, nil
}
