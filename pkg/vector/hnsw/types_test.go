package hnsw_test

import (
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

func TestQueryConfigValidate(t *testing.T) {
	tests := []struct {
		name   string
		config hnsw.QueryConfig
		valid  bool
	}{
		{name: "defaults", config: hnsw.QueryConfig{}, valid: true},
		{name: "explicit values", config: hnsw.QueryConfig{EfSearch: 1, VisitLimit: 1}, valid: true},
		{name: "negative ef search", config: hnsw.QueryConfig{EfSearch: -1}},
		{name: "negative visit limit", config: hnsw.QueryConfig{VisitLimit: -1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if test.valid && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if !test.valid && !errors.Is(err, hnsw.ErrInvalidQueryConfig) {
				t.Fatalf("Validate() error = %v, want ErrInvalidQueryConfig", err)
			}
		})
	}
}
