package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
)

type listedModel struct {
	Name   string `json:"name"`
	Model  string `json:"model"`
	Digest string `json:"digest"`
}

type showResponse struct {
	Capabilities []string                   `json:"capabilities"`
	ModelInfo    map[string]json.RawMessage `json:"model_info"`
}

func (c *Client) resolveAndValidateModel(ctx context.Context, baseURL string) error {
	digest, err := c.resolveDigest(ctx, baseURL)
	if err != nil {
		return err
	}

	var shown showResponse
	if err := c.requestJSON(
		ctx,
		http.MethodPost,
		baseURL+"/api/show",
		map[string]string{"model": c.model.ID},
		&shown,
	); err != nil {
		return fmt.Errorf("embedding/ollama: inspect model: %w", err)
	}
	if len(shown.Capabilities) > 0 && !slices.Contains(shown.Capabilities, "embedding") {
		return fmt.Errorf("%w: model %q does not declare embedding capability", ErrModelMismatch, c.model.ID)
	}
	var architecture string
	if err := json.Unmarshal(shown.ModelInfo["general.architecture"], &architecture); err != nil || architecture == "" {
		return fmt.Errorf("%w: missing model architecture", ErrModelMismatch)
	}
	dimensions, err := modelInfoInteger(shown.ModelInfo, architecture+".embedding_length")
	if err != nil || dimensions != c.model.Dimensions {
		return fmt.Errorf("%w: dimensions %d, want %d", ErrModelMismatch, dimensions, c.model.Dimensions)
	}
	contextTokens, err := modelInfoInteger(shown.ModelInfo, architecture+".context_length")
	if err != nil || contextTokens != c.model.ContextTokens {
		return fmt.Errorf("%w: context tokens %d, want %d", ErrModelMismatch, contextTokens, c.model.ContextTokens)
	}
	verifiedDigest, err := c.resolveDigest(ctx, baseURL)
	if err != nil {
		return err
	}
	if verifiedDigest != digest {
		return fmt.Errorf("%w: model revision changed during initialization", ErrModelMismatch)
	}
	c.model.Revision = digest
	if err := c.model.Validate(); err != nil {
		return fmt.Errorf("%w: resolved model is invalid", ErrModelMismatch)
	}
	return nil
}

func (c *Client) resolveDigest(ctx context.Context, baseURL string) (string, error) {
	var listed struct {
		Models []listedModel `json:"models"`
	}
	if err := c.requestJSON(ctx, http.MethodGet, baseURL+"/api/tags", nil, &listed); err != nil {
		return "", fmt.Errorf("embedding/ollama: list models: %w", err)
	}
	for _, candidate := range listed.Models {
		if candidate.Name == c.model.ID || candidate.Model == c.model.ID {
			if candidate.Digest == "" {
				return "", fmt.Errorf("%w: model %q has no digest", ErrModelMismatch, c.model.ID)
			}
			return candidate.Digest, nil
		}
	}
	return "", fmt.Errorf("%w: model %q was not returned by /api/tags", ErrModelMismatch, c.model.ID)
}

func modelInfoInteger(info map[string]json.RawMessage, key string) (int, error) {
	var value int
	if err := json.Unmarshal(info[key], &value); err != nil || value <= 0 {
		return 0, ErrModelMismatch
	}
	return value, nil
}

func (c *Client) requestJSON(ctx context.Context, method, endpoint string, input, output any) error {
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return err
	}
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, c.maxResponseBytes+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > c.maxResponseBytes {
		return errors.New("response exceeds configured limit")
	}
	if response.StatusCode != http.StatusOK {
		return &APIError{StatusCode: response.StatusCode, Body: strings.TrimSpace(string(data))}
	}
	if err := json.Unmarshal(data, output); err != nil {
		return err
	}
	return nil
}
