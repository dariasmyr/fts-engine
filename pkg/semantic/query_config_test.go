package semantic

import (
	"errors"
	"testing"
)

func TestQueryConfigDelegatesHNSWValidation(t *testing.T) {
	tests := []struct {
		name   string
		config QueryConfig
	}{
		{name: "negative ef search", config: QueryConfig{EfSearch: -1}},
		{name: "negative visit limit", config: QueryConfig{VisitLimit: -1}},
		{name: "negative candidate chunks", config: QueryConfig{CandidateChunks: -1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.config.Validate(); !errors.Is(err, ErrInvalidQueryConfig) {
				t.Fatalf("Validate() error = %v, want ErrInvalidQueryConfig", err)
			}
		})
	}
}
