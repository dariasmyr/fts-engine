package semantic

import (
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestDefaultSemanticConfigReturnsEffectiveConfig(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config, err := DefaultSemanticConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	if err := config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	wantChunking := chunk.Config{TargetBytes: 384, MaxBytes: 512, OverlapBytes: 64}
	if config.Index.Metric != vector.MetricCosine || config.Chunking.Builtin != wantChunking {
		t.Fatalf("default config metric/chunking = %v/%+v", config.Index.Metric, config.Chunking.Builtin)
	}
	if config.Index.Limits.MaxStaleVectors != defaultMaxLiveVectors || config.Index.Limits.MaxSegments != 16 {
		t.Fatalf("default normalized limits = %+v", config.Index.Limits)
	}
}

func TestSemanticConfigDelegatesHNSWValidation(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config, err := DefaultSemanticConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	config.Index.HNSW.MaxNeighbors = 1025
	config.Index.HNSW.EfConstruction = 1025
	if err := config.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Validate() error = %v, want ErrInvalidConfig", err)
	}
}
