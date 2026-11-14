package cui

import (
	"strings"
	"testing"
	"time"

	"github.com/dariasmyr/fts-engine/demo/internal/domain/models"
)

func TestModelSectionSystemUsesModeSpecificIndexDetails(t *testing.T) {
	t.Parallel()

	info := Info{
		Engine:         "lexical-engine",
		Index:          "lexical-index",
		Scorer:         "lexical-scorer",
		Profile:        "lexical-profile",
		Filter:         "lexical-filter",
		SemanticEngine: "semantic-engine",
		SemanticIndex:  "semantic-index",
		Version:        "test-version",
	}
	tests := []struct {
		name      string
		mode      searchMode
		want      []string
		doNotWant []string
	}{
		{
			name:      "syntax shows lexical details",
			mode:      searchModeSyntax,
			want:      []string{"lexical-engine", "lexical-index", "lexical-scorer", "lexical-profile", "lexical-filter", "title"},
			doNotWant: []string{"semantic-engine", "semantic-index"},
		},
		{
			name:      "semantic hides lexical details",
			mode:      searchModeSemantic,
			want:      []string{"semantic-engine", "semantic-index", "test-version"},
			doNotWant: []string{"lexical-engine", "lexical-index", "lexical-scorer", "lexical-profile", "lexical-filter", "shape", "fields"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			output := (model{
				mode:      tt.mode,
				info:      info,
				fields:    []string{"title"},
				startTime: now,
				now:       now,
			}).sectionSystem(40)

			for _, value := range tt.want {
				if !strings.Contains(output, value) {
					t.Errorf("system section omitted %q:\n%s", value, output)
				}
			}
			for _, value := range tt.doNotWant {
				if strings.Contains(output, value) {
					t.Errorf("system section contains unrelated %q:\n%s", value, output)
				}
			}
		})
	}
}

func TestModelSectionDiagnosticsUsesSemanticMetrics(t *testing.T) {
	t.Parallel()

	output := (model{diagnostics: &models.SearchDiagnostics{
		LogicalQueryType:     "semantic",
		ExecutionStrategy:    "hnsw",
		IndexSearches:        2,
		ConfiguredEfSearch:   8,
		AppliedEfSearch:      5,
		CandidateChunks:      4,
		CandidateDocs:        3,
		VisitedNodes:         12,
		DistanceComputations: 12,
		RejectedNodes:        2,
		MatchedDocs:          3,
		ReturnedDocs:         2,
	}}).sectionDiagnostics(40)

	for _, value := range []string{
		"configured_ef", "applied_ef", "candidate_chunks", "candidate_docs",
		"visited_nodes", "distances", "rejected_nodes",
	} {
		if !strings.Contains(output, value) {
			t.Errorf("semantic diagnostics omitted %q:\n%s", value, output)
		}
	}
	for _, value := range []string{"tokens", "fields", "keys", "checks", "postings"} {
		if strings.Contains(output, value) {
			t.Errorf("semantic diagnostics contains lexical metric %q:\n%s", value, output)
		}
	}
}
