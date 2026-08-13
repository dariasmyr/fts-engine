package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/dariasmyr/fts-engine/demo/internal/adapters/cui"
	"github.com/dariasmyr/fts-engine/demo/internal/domain/models"
	pkgfts "github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsstats"
)

func runCUI(ctx context.Context, log *slog.Logger, cfgIndex string, cfgScorer string, cfgProfile string, cfgFilter string, engine *serviceAdapter, documentsByID map[string]models.Document) error {
	appCUI := cui.New(ctx, log, engine, documentsByID, 10, cui.Info{
		Engine:  "pkg/fts",
		Index:   cfgIndex,
		Scorer:  cfgScorer,
		Profile: cfgProfile,
		Filter:  cfgFilter,
		Version: buildVersion(),
	})

	if err := appCUI.Start(); err != nil {
		return err
	}
	if snapshot, ok := engine.SearchStatsSnapshot(); ok && snapshot.TotalSearches > 0 {
		logSearchStats(log, snapshot)
	}
	return nil
}

type serviceAdapter struct {
	service        *pkgfts.Service
	snapshotLoaded bool
	searchStats    *ftsstats.SearchStats
}

func (s *serviceAdapter) Fields() []string {
	if s == nil || s.service == nil {
		return nil
	}
	return s.service.Fields()
}

func (s *serviceAdapter) HighlightPlainText(query string, text string) string {
	if s == nil || s.service == nil || strings.TrimSpace(query) == "" || text == "" {
		return text
	}

	fragments := s.service.HighlightPlainText(query, text, pkgfts.Highlighter{
		PreTag:       "\033[31m",
		PostTag:      "\033[0m",
		MaxFragments: 3,
		FragmentSize: 180,
		Separator:    " ... ",
	})
	if len(fragments) == 0 {
		return text
	}

	out := make([]string, 0, len(fragments))
	for _, fragment := range fragments {
		out = append(out, fragment.Text)
	}
	return strings.Join(out, "\n")
}

func (s *serviceAdapter) HighlightQueryString(query string, text string) string {
	if s == nil || s.service == nil || strings.TrimSpace(query) == "" || text == "" {
		return text
	}

	fragments := s.service.Highlight(query, text, pkgfts.Highlighter{
		PreTag:       "\033[31m",
		PostTag:      "\033[0m",
		MaxFragments: 3,
		FragmentSize: 180,
		Separator:    " ... ",
	})
	if len(fragments) == 0 {
		return text
	}

	out := make([]string, 0, len(fragments))
	for _, fragment := range fragments {
		out = append(out, fragment.Text)
	}
	return strings.Join(out, "\n")
}

func (s *serviceAdapter) SearchPlainText(ctx context.Context, query string, maxResults int) (*models.SearchResult, error) {
	ctx = pkgfts.WithDiagnostics(ctx)
	result, err := s.service.SearchPlainText(ctx, query, maxResults)
	if s.searchStats != nil {
		s.searchStats.ObserveResult(query, result, err)
	}
	if err != nil {
		return nil, err
	}

	return &models.SearchResult{
		ResultData:        s.projectResults(ctx, query, result, false),
		TotalResultsCount: result.TotalResultsCount,
		Diagnostics:       projectDiagnostics(result.Diagnostics),
	}, nil
}

func (s *serviceAdapter) SearchQueryString(ctx context.Context, query string, maxResults int) (*models.SearchResult, error) {
	ctx = pkgfts.WithDiagnostics(ctx)
	result, err := s.service.SearchDocuments(ctx, query, maxResults)
	if s.searchStats != nil {
		s.searchStats.ObserveResult(query, result, err)
	}
	if err != nil {
		return nil, err
	}

	return &models.SearchResult{
		ResultData:        s.projectResults(ctx, query, result, true),
		TotalResultsCount: result.TotalResultsCount,
		Diagnostics:       projectDiagnostics(result.Diagnostics),
	}, nil
}

func (s *serviceAdapter) projectResults(ctx context.Context, query string, result *pkgfts.SearchResult, explain bool) []models.ResultData {
	if result == nil {
		return nil
	}
	out := make([]models.ResultData, 0, len(result.Results))
	for _, item := range result.Results {
		data := models.ResultData{
			ID:            string(item.ID),
			UniqueMatches: item.UniqueMatches,
			TotalMatches:  item.TotalMatches,
			Score:         item.Score,
		}
		if explain && s != nil && s.service != nil {
			data.Explanation = s.projectExplanation(ctx, query, item.ID)
		}
		out = append(out, data)
	}
	return out
}

func (s *serviceAdapter) projectExplanation(ctx context.Context, query string, id pkgfts.DocID) *models.Explain {
	explanation, err := s.service.Explain(ctx, query, id)
	if err != nil || explanation == nil || !explanation.Matched {
		return nil
	}
	contributions := make([]models.ScoreContribution, 0, len(explanation.Contributions))
	for _, c := range explanation.Contributions {
		contributions = append(contributions, models.ScoreContribution{
			Field:           c.Field,
			Term:            c.Term,
			QueryType:       string(c.QueryType),
			TF:              c.TF,
			DF:              c.DF,
			BaseScore:       c.BaseScore,
			FieldWeight:     c.FieldWeight,
			QueryTypeWeight: c.QueryTypeWeight,
			Score:           c.Score,
		})
	}
	return &models.Explain{
		Score:         explanation.Score,
		UniqueMatches: explanation.UniqueMatches,
		TotalMatches:  explanation.TotalMatches,
		Contributions: contributions,
	}
}

func (s *serviceAdapter) SearchStatsSnapshot() (ftsstats.Snapshot, bool) {
	if s.searchStats == nil {
		return ftsstats.Snapshot{}, false
	}
	return s.searchStats.Snapshot(), true
}

func logSearchStats(log *slog.Logger, snapshot ftsstats.Snapshot) {
	log.Info("search diagnostics summary",
		"total_searches", snapshot.TotalSearches,
		"errors_total", snapshot.ErrorsTotal,
		"zero_results", snapshot.ZeroResults,
		"strategies", len(snapshot.ByStrategy),
	)
	for strategy, stats := range snapshot.ByStrategy {
		log.Info("search diagnostics strategy",
			"strategy", strategy,
			"count", stats.Count,
			"avg_duration", stats.AvgDuration(),
			"max_duration", stats.MaxDuration,
			"avg_postings", stats.AvgPostings(),
		)
	}
}

func formatDiagnosticsTimings(diag *pkgfts.QueryDiagnostics) map[string]string {
	if diag == nil {
		return map[string]string{}
	}
	out := make(map[string]string, 3)
	if diag.Timings.HasPreprocess() {
		out["preprocess"] = formatAppDuration(diag.Timings.Preprocess)
	}
	if diag.Timings.HasSearchTokens() {
		out["search_tokens"] = formatAppDuration(diag.Timings.SearchTokens)
	}
	if diag.Timings.HasTotal() {
		out["total"] = formatAppDuration(diag.Timings.Total)
	}
	return out
}

func projectDiagnostics(diag *pkgfts.QueryDiagnostics) *models.SearchDiagnostics {
	if diag == nil {
		return nil
	}
	return &models.SearchDiagnostics{
		LogicalQueryType:   diag.LogicalQueryType,
		ExecutionStrategy:  diag.ExecutionStrategy,
		StrategySkipReason: diag.StrategySkipReason,
		Timings:            formatDiagnosticsTimings(diag),
		ProcessedTokens:    diag.ProcessedTokens,
		FieldsVisited:      diag.FieldsVisited,
		GeneratedKeys:      diag.GeneratedKeys,
		IndexSearches:      diag.IndexSearches,
		FilterChecks:       diag.FilterChecks,
		FilterRejects:      diag.FilterRejects,
		PostingEntriesRead: diag.PostingEntriesRead,
		CandidateDocs:      diag.CandidateDocs,
		MatchedDocs:        diag.MatchedDocs,
		ReturnedDocs:       diag.ReturnedDocs,
	}
}

func formatAppDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dus", d.Microseconds())
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}
