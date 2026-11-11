package semantic

import (
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestDefaultServiceConfigReturnsEffectiveConfig(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config, err := DefaultServiceConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	if err := config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	wantChunking := chunk.Config{TargetBytes: 384, MaxBytes: 512, OverlapBytes: 64}
	if config.Index.Dimensions != client.Model().Dimensions ||
		config.Index.Metric != vector.MetricCosine || config.Chunking != wantChunking {
		t.Fatalf("default config index/chunking = %+v/%+v", config.Index, config.Chunking)
	}
	if config.Index.Limits.MaxStaleVectors != defaultMaxLiveVectors || config.Index.Limits.MaxSegments != 16 {
		t.Fatalf("default normalized limits = %+v", config.Index.Limits)
	}
	if config.Index.Search.MaxQueryChunks != defaultMaxChunksPerDocument ||
		config.Index.Search.MaxEfSearch != defaultMaxChunkCandidates ||
		config.Index.Search.MaxVisitLimit != defaultMaxLiveVectors {
		t.Fatalf("default search policy = %+v", config.Index.Search)
	}
	if config.Encoding != (EncodingLimits{
		MaxFields: defaultMaxFields, MaxSourceBytes: defaultMaxSourceBytes,
		MaxEmbeddingBytes: uint64(defaultMaxChunksPerDocument * client.Model().Dimensions * 4),
	}) {
		t.Fatalf("default encoding limits = %+v", config.Encoding)
	}
}

func TestServiceConfigDelegatesHNSWValidation(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config, err := DefaultServiceConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	config.Index.HNSW.Build.MaxNeighbors = 1025
	config.Index.HNSW.Build.EfConstruction = 1025
	if err := config.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Validate() error = %v, want ErrInvalidConfig", err)
	}
}

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
