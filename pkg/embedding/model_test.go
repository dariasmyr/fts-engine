package embedding

import (
	"context"
	"errors"
	"math"
	"testing"
)

func TestModelValidate(t *testing.T) {
	valid := Model{ID: "model", Revision: "sha256:digest", Dimensions: 2, ContextTokens: 512}
	tests := []struct {
		name  string
		model Model
	}{
		{name: "valid", model: valid},
		{name: "missing id", model: Model{Revision: valid.Revision, Dimensions: 2, ContextTokens: 512}},
		{name: "missing revision", model: Model{ID: valid.ID, Dimensions: 2, ContextTokens: 512}},
		{name: "missing dimensions", model: Model{ID: valid.ID, Revision: valid.Revision, ContextTokens: 512}},
		{name: "missing context", model: Model{ID: valid.ID, Revision: valid.Revision, Dimensions: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.model.Validate()
			if tt.name == "valid" && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if tt.name != "valid" && !errors.Is(err, ErrInvalidModel) {
				t.Fatalf("Validate() error = %v, want ErrInvalidModel", err)
			}
		})
	}
}

func TestNewClientCopiesModel(t *testing.T) {
	model := Model{ID: "model", Revision: "digest", Dimensions: 2, ContextTokens: 512}
	client, err := NewClient(model, func(context.Context, []string) ([][]float32, error) {
		return [][]float32{{1, 2}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	model.ID = "changed"
	returned := client.Model()
	returned.ID = "also-changed"
	if got := client.Model().ID; got != "model" {
		t.Fatalf("Model().ID = %q, want model", got)
	}
}

func TestNewClientRejectsInvalidInput(t *testing.T) {
	valid := Model{ID: "model", Revision: "digest", Dimensions: 2, ContextTokens: 512}
	if _, err := NewClient(Model{}, func(context.Context, []string) ([][]float32, error) { return nil, nil }); !errors.Is(err, ErrInvalidModel) {
		t.Fatalf("NewClient(invalid model) error = %v, want ErrInvalidModel", err)
	}
	if _, err := NewClient(valid, nil); !errors.Is(err, ErrNilClient) {
		t.Fatalf("NewClient(nil) error = %v, want ErrNilClient", err)
	}
}

func TestBoundClientValidatesResults(t *testing.T) {
	model := Model{ID: "model", Revision: "digest", Dimensions: 2, ContextTokens: 512}
	tests := []struct {
		name    string
		vectors [][]float32
	}{
		{name: "wrong dimensions", vectors: [][]float32{{1}}},
		{name: "non-finite", vectors: [][]float32{{1, float32(math.NaN())}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClient(model, func(context.Context, []string) ([][]float32, error) {
				return tt.vectors, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.Embed(t.Context(), []string{"text"}); !errors.Is(err, ErrInvalidResult) {
				t.Fatalf("Embed() error = %v, want ErrInvalidResult", err)
			}
		})
	}
}
