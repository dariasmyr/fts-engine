package semanticpersist_test

import (
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestDefaultConfigAcceptsValidSemanticCapacity(t *testing.T) {
	t.Parallel()

	index, err := semantic.NewIndex(semantic.IndexConfig{
		Dimensions: 768,
		Metric:     vector.MetricCosine,
		Limits: semantic.LimitsConfig{
			MaxLiveVectors:       1_000_000,
			MaxStaleVectors:      1_000_000,
			MaxSegments:          16,
			MaxChunksPerDocument: 64,
		},
		Search: semantic.SearchConfig{
			MaxResults: 20, MaxCandidateChunks: 200, MaxChunksPerResult: 3,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := semanticpersist.Save(
		t.Context(),
		t.TempDir(),
		index,
		semanticpersist.SaveConfig{
			Config:             semanticpersist.DefaultConfig(),
			ExpectedGeneration: &semanticpersist.Generation{},
		},
	); err != nil {
		t.Fatalf("Save() with default persistence config: %v", err)
	}
}
