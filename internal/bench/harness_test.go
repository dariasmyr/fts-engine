package bench

import (
	"context"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/internal/domain/models"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/radix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func mkDoc(id, title, abstract string) models.Document {
	return models.Document{
		DocumentBase: models.DocumentBase{Title: title, Abstract: abstract},
		ID:           id,
	}
}

func TestCorpusTitleIndex(t *testing.T) {
	corpus := Corpus{
		mkDoc("1", "  Rosa Barge  ", "french hotel barge"),
		mkDoc("2", "Obama Speech", "presidential address"),
		mkDoc("3", "", "no title"),
	}

	idx := corpus.TitleIndex()
	if got, want := idx["rosa barge"], "1"; got != want {
		t.Fatalf("idx[rosa barge] = %q, want %q", got, want)
	}
	if _, ok := idx[""]; ok {
		t.Fatal("empty title should not be indexed")
	}
}

func TestResolveRelevant(t *testing.T) {
	titleIdx := map[string]string{"rosa barge": "1", "obama speech": "2"}
	q := Query{
		RelevantIDs:    []string{"raw-id-3"},
		RelevantTitles: []string{"Rosa Barge", "Unknown Title"},
	}

	got := ResolveRelevant(q, titleIdx)
	want := []string{"raw-id-3", "1"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("ResolveRelevant() = %v, want %v", got, want)
	}
}

func TestCountMissingTitles(t *testing.T) {
	titleIdx := map[string]string{"rosa barge": "1"}
	gt := &GroundTruth{Queries: []Query{
		{RelevantTitles: []string{"Rosa Barge", "Unknown"}},
		{RelevantTitles: []string{"Another Unknown"}},
	}}

	if got := CountMissingTitles(gt, titleIdx); got != 2 {
		t.Fatalf("CountMissingTitles() = %d, want 2", got)
	}
}

func TestIndexAndRunQueriesEndToEnd(t *testing.T) {
	ctx := context.Background()
	corpus := Corpus{
		mkDoc("1", "Rosa Barge", "Rosa is a French hotel barge on the Canal du Midi"),
		mkDoc("2", "Barack Obama", "Obama delivered a speech on climate change"),
		mkDoc("3", "Empty", "unrelated content about cats and dogs"),
	}

	svc := fts.New(radix.New(), keygen.Word)
	idxReport, err := IndexCorpus(ctx, svc, corpus, SelectAbstract)
	if err != nil {
		t.Fatalf("IndexCorpus() error = %v", err)
	}
	if idxReport.DocumentCount != 3 {
		t.Fatalf("DocumentCount = %d, want 3", idxReport.DocumentCount)
	}

	gt := &GroundTruth{Queries: []Query{
		{Query: "french hotel barge", RelevantTitles: []string{"Rosa Barge"}},
		{Query: "obama speech", RelevantTitles: []string{"Barack Obama"}},
	}}

	titleIdx := corpus.TitleIndex()
	queryReports, err := RunQueries(ctx, svc, gt, titleIdx, 10)
	if err != nil {
		t.Fatalf("RunQueries() error = %v", err)
	}
	if len(queryReports) != 2 {
		t.Fatalf("len(queryReports) = %d, want 2", len(queryReports))
	}

	report := Aggregate(10, idxReport, queryReports)
	if report.MeanNDCG <= 0 {
		t.Fatalf("MeanNDCG = %v, want > 0", report.MeanNDCG)
	}
	if report.MeanMRR <= 0 {
		t.Fatalf("MeanMRR = %v, want > 0", report.MeanMRR)
	}
}
