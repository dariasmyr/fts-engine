package semanticpersist

import (
	"errors"
	"reflect"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
)

func TestRuntimeConfigWireMappingRoundTrip(t *testing.T) {
	t.Parallel()

	config, err := service.DefaultConfig(embedding.Model{
		ID: "model", Revision: "v1", Dimensions: 4, ContextTokens: 512,
		DocumentPrefix: "document: ", QueryPrefix: "query: ",
	})
	if err != nil {
		t.Fatal(err)
	}

	index, err := indexConfigFromFormat(indexConfigToFormat(config.Index))
	if err != nil {
		t.Fatal(err)
	}
	if index != config.Index {
		t.Fatalf("index config round trip = %+v, want %+v", index, config.Index)
	}
	pipeline, err := pipelineConfigFromFormat(pipelineConfigToFormat(config.Pipeline), index)
	if err != nil {
		t.Fatal(err)
	}
	if pipeline != config.Pipeline {
		t.Fatalf("pipeline config round trip = %+v, want %+v", pipeline, config.Pipeline)
	}
}

func TestSegmentStateWireMappingRoundTrip(t *testing.T) {
	t.Parallel()

	state := semantic.SegmentState{
		ID: 7,
		Rows: []semantic.VectorRow{{
			VectorID: 11,
			Chunk: semantic.Ref{
				ID: "chunk", DocID: fts.DocID("doc"), Field: "body", Ordinal: 2, StartByte: 3, EndByte: 9,
			},
		}},
		LivenessWords: []uint64{1},
	}
	decoded := segmentStateFromFormat(segmentStateToFormat(state))
	if !reflect.DeepEqual(decoded, state) {
		t.Fatalf("segment state round trip = %+v, want %+v", decoded, state)
	}
}

func TestValidateWireStateRejectsRuntimeContractMismatch(t *testing.T) {
	t.Parallel()

	config, err := service.DefaultConfig(embedding.Model{ID: "model", Revision: "v1", Dimensions: 4, ContextTokens: 512})
	if err != nil {
		t.Fatal(err)
	}
	pipeline := pipelineConfigToFormat(config.Pipeline)
	state := generationformat.IndexState{
		ID:            [16]byte{1},
		Config:        indexConfigToFormat(config.Index),
		Pipeline:      &pipeline,
		NextSegmentID: 1,
	}

	state.Pipeline.EncodingBehaviorVersion++
	if err := validateFormatState(state); !errors.Is(err, generationformat.ErrUnsupportedVersion) {
		t.Fatalf("validateFormatState() error = %v, want ErrUnsupportedVersion", err)
	}

	state.Pipeline.EncodingBehaviorVersion = service.CurrentEncodingBehaviorVersion
	state.Config.Metric = 255
	if err := validateFormatState(state); !errors.Is(err, generationformat.ErrCorrupt) {
		t.Fatalf("validateFormatState() error = %v, want ErrCorrupt", err)
	}
}
