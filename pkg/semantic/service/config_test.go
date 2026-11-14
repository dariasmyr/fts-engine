package service

import (
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestDefaultConfigReturnsEffectiveConfig(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config, err := DefaultConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	if err := config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	wantChunking := chunk.Config{TargetBytes: 384, MaxBytes: 512, OverlapBytes: 64}
	if config.Index.Dimensions != client.Model().Dimensions ||
		config.Index.Metric != vector.MetricCosine || config.Pipeline.Chunking != wantChunking ||
		config.Pipeline.EncodingBehaviorVersion != CurrentEncodingBehaviorVersion {
		t.Fatalf("default config index/chunking = %+v/%+v", config.Index, config.Pipeline.Chunking)
	}
	if config.Index.Operational.Capacity.MaxStaleVectors != defaultMaxLiveVectors || config.Index.Operational.Capacity.MaxSegments != 16 {
		t.Fatalf("default normalized limits = %+v", config.Index.Operational.Capacity)
	}
	if config.Index.Operational.Search.MaxQueryChunks != defaultMaxChunksPerDocument ||
		config.Index.Operational.Search.MaxEfSearch != defaultMaxChunkCandidates ||
		config.Index.Operational.Search.MaxVisitLimit != defaultMaxLiveVectors {
		t.Fatalf("default search policy = %+v", config.Index.Operational.Search)
	}
	if config.Pipeline.Operational.Encoding != (EncodingLimits{
		MaxFields: defaultMaxFields, MaxSourceBytes: defaultMaxSourceBytes,
		MaxEmbeddingBytes: uint64(defaultMaxChunksPerDocument * client.Model().Dimensions * 4),
	}) {
		t.Fatalf("default encoding limits = %+v", config.Pipeline.Operational.Encoding)
	}
}

func TestConfigRejectsUnsupportedEncodingBehavior(t *testing.T) {
	t.Parallel()

	client, _ := modelServiceTestClient(t, 512)
	config, err := DefaultConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	config.Pipeline.EncodingBehaviorVersion++
	if err := config.Validate(); !errors.Is(err, ErrUnsupportedEncodingBehavior) {
		t.Fatalf("Validate() error = %v, want ErrUnsupportedEncodingBehavior", err)
	}
}

func TestConfigDelegatesHNSWValidation(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config, err := DefaultConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	config.Index.Build.MaxNeighbors = 1025
	config.Index.Build.EfConstruction = 1025
	if err := config.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Validate() error = %v, want ErrInvalidConfig", err)
	}
}
