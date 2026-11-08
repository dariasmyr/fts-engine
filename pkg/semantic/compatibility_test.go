package semantic

import (
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestDeriveSchemaSeparatesDocumentAndQueryCompatibility(t *testing.T) {
	chunking := ChunkingSchema{ID: "byte-splitter", Version: 1, Fingerprint: "v1:chunks"}
	model := embedding.Model{
		ID: "model", Revision: "digest", Dimensions: 2, ContextTokens: 512,
		DocumentPrefix: "document: ", QueryPrefix: "query: ",
	}
	schema, err := DeriveSchema(model, chunking, vector.MetricCosine)
	if err != nil {
		t.Fatal(err)
	}
	retrieval, err := DeriveRetrievalFingerprint(schema, model)
	if err != nil {
		t.Fatal(err)
	}

	queryChanged := model
	queryChanged.QueryPrefix = "different query: "
	querySchema, err := DeriveSchema(queryChanged, chunking, vector.MetricCosine)
	if err != nil {
		t.Fatal(err)
	}
	if querySchema != schema {
		t.Fatalf("query-only change altered persisted schema: %+v != %+v", querySchema, schema)
	}
	changedRetrieval, err := DeriveRetrievalFingerprint(schema, queryChanged)
	if err != nil {
		t.Fatal(err)
	}
	if changedRetrieval == retrieval {
		t.Fatal("query-only change did not alter retrieval fingerprint")
	}

	documentChanged := model
	documentChanged.DocumentPrefix = "different document: "
	documentSchema, err := DeriveSchema(documentChanged, chunking, vector.MetricCosine)
	if err != nil {
		t.Fatal(err)
	}
	if documentSchema == schema {
		t.Fatal("document prefix change did not alter persisted schema")
	}
}

func TestDeriveSchemaChangesWithPersistedInputs(t *testing.T) {
	baseModel := embedding.Model{ID: "model", Revision: "digest", Dimensions: 2, ContextTokens: 512}
	baseChunking := ChunkingSchema{ID: "byte-splitter", Version: 1, Fingerprint: "v1:chunks"}
	base, err := DeriveSchema(baseModel, baseChunking, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		model    embedding.Model
		chunking ChunkingSchema
		metric   vector.Metric
	}{
		{name: "revision", model: embedding.Model{ID: "model", Revision: "other", Dimensions: 2, ContextTokens: 512}, chunking: baseChunking, metric: vector.MetricL2Squared},
		{name: "chunking", model: baseModel, chunking: ChunkingSchema{ID: "byte-splitter", Version: 1, Fingerprint: "v1:other"}, metric: vector.MetricL2Squared},
		{name: "metric", model: baseModel, chunking: baseChunking, metric: vector.MetricCosine},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DeriveSchema(tt.model, tt.chunking, tt.metric)
			if err != nil {
				t.Fatal(err)
			}
			if got == base {
				t.Fatalf("DeriveSchema() did not change for %s", tt.name)
			}
		})
	}
}

func TestSchemaRejectsUnknownCompatibilityVersion(t *testing.T) {
	model := embedding.Model{ID: "model", Revision: "digest", Dimensions: 2, ContextTokens: 512}
	schema, err := DeriveSchema(
		model,
		ChunkingSchema{ID: "whole-field", Version: 1, Fingerprint: "v1:whole"},
		vector.MetricCosine,
	)
	if err != nil {
		t.Fatal(err)
	}
	schema.Embedding.CompatibilityVersion++
	if schema.IsValid() {
		t.Fatal("Schema.IsValid() = true for unknown compatibility version")
	}
}
