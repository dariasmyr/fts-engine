package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftseval"
)

func TestParseFlagsRequiresProfiles(t *testing.T) {
	if _, err := parseFlags(nil); err == nil {
		t.Fatalf("expected missing profiles error")
	}
}

func TestParseFlagsAcceptsProfiles(t *testing.T) {
	cfg, err := parseFlags([]string{
		"-profiles=./profiles/a.json,./profiles/b.json",
		"-baseline=tfidf",
		"-queries=12",
		"-k=5",
	})
	if err != nil {
		t.Fatalf("parseFlags() error = %v", err)
	}
	if cfg.Baseline != "tfidf" || cfg.Queries != 12 || cfg.K != 5 || len(cfg.Profiles) != 2 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestTitleWeightedProfileImprovesSyntheticRanking(t *testing.T) {
	docs, queries := multifieldSynthetic(20)
	ctx := context.Background()

	baseline, err := evaluate(ctx, docs, queries, 10, fts.WithScorer(fts.BM25()))
	if err != nil {
		t.Fatalf("evaluate baseline: %v", err)
	}
	candidate, err := evaluate(ctx, docs, queries, 10, fts.WithRankProfile(fts.RankProfile{
		Name: "title-medium",
		Base: fts.BM25(),
		FieldWeights: fts.FieldWeights{
			"title": 3,
			"tags":  1.5,
			"body":  1,
		},
	}))
	if err != nil {
		t.Fatalf("evaluate candidate: %v", err)
	}

	if candidate.NDCGAtK <= baseline.NDCGAtK {
		t.Fatalf("candidate nDCG = %v, baseline = %v", candidate.NDCGAtK, baseline.NDCGAtK)
	}
	if candidate.MRR <= baseline.MRR {
		t.Fatalf("candidate MRR = %v, baseline = %v", candidate.MRR, baseline.MRR)
	}
}

func TestLoadRankProfileRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "profile.json")
	if err := os.WriteFile(path, []byte(`{
  "name": "bad-profile",
  "base": "bm25",
  "field_weights": {"title": 3.0},
  "match_weights": {"phrase": 4.0}
}`), 0o644); err != nil {
		t.Fatalf("write profile: %v", err)
	}
	if _, err := loadRankProfile(path); err == nil {
		t.Fatalf("expected unknown field error")
	}
}

func TestCompareQueriesCountsNDCGMovement(t *testing.T) {
	baseline := &ftseval.Report{Queries: []ftseval.QueryReport{
		{Name: "same", NDCGAtK: 0.5},
		{Name: "better", NDCGAtK: 0.5},
		{Name: "worse", NDCGAtK: 0.5},
	}}
	candidate := &ftseval.Report{Queries: []ftseval.QueryReport{
		{Name: "same", NDCGAtK: 0.5},
		{Name: "better", NDCGAtK: 0.75},
		{Name: "worse", NDCGAtK: 0.25},
	}}

	got := compareQueries(candidate, baseline)
	if got.Improved != 1 || got.Worse != 1 || got.Same != 1 {
		t.Fatalf("movement counts = %+v, want 1/1/1", got)
	}
	if got.MaxNDCGGain != 0.25 || got.MaxNDCGLoss != -0.25 {
		t.Fatalf("movement extrema = %+v, want +/-0.25", got)
	}
}

func TestWikiMultifieldGeneratesTitleBodyQueries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wiki.xml")
	if err := os.WriteFile(path, []byte(`<mediawiki>
  <page><title>Postgres Backup</title><id>1</id><revision><text>database archive guide</text></revision></page>
  <page><title>Archive Guide</title><id>2</id><revision><text>postgres backup backup backup restore</text></revision></page>
  <page><title>Restore Notes</title><id>3</id><revision><text>postgres maintenance backup</text></revision></page>
</mediawiki>`), 0o644); err != nil {
		t.Fatalf("write wiki fixture: %v", err)
	}

	docs, queries, err := wikiMultifield(path, 10, 5)
	if err != nil {
		t.Fatalf("wikiMultifield() error = %v", err)
	}
	if len(docs) != 3 {
		t.Fatalf("docs = %d, want 3", len(docs))
	}
	if len(queries) == 0 {
		t.Fatalf("expected generated queries")
	}

	foundPostgres := false
	for _, q := range queries {
		if q.Query != "postgres" {
			continue
		}
		foundPostgres = true
		if q.Relevant["1"] != 3 {
			t.Fatalf("title doc relevance = %v, want 3", q.Relevant["1"])
		}
		if q.Relevant["2"] != 1 || q.Relevant["3"] != 1 {
			t.Fatalf("body relevance = %+v, want body-only docs at 1", q.Relevant)
		}
	}
	if !foundPostgres {
		t.Fatalf("expected postgres query, got %+v", queries)
	}
}
