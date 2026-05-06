package fts

import (
	"context"
	"sync"
	"time"
)

type QueryDiagnostics struct {
	// LogicalQueryType describes the parsed logical query form: term, prefix, phrase, boolean, etc.
	LogicalQueryType string
	// ExecutionStrategy describes the physical execution path that was selected.
	ExecutionStrategy string
	// StrategyReason stores the first useful reason for strategy selection or fast-path skip.
	StrategyReason string

	// ProcessedTokens is the total number of tokens processed across the executed query path.
	ProcessedTokens int
	// FieldsVisited is the accumulated field fan-out across the executed query path.
	FieldsVisited int
	// GeneratedKeys is the total number of index keys generated from processed tokens.
	GeneratedKeys int
	// IndexSearches counts index/prefix/positional search calls executed by the query.
	IndexSearches int
	// FilterChecks counts membership checks against the optional filter.
	FilterChecks int
	// FilterRejects counts filter checks that rejected a key before index lookup.
	FilterRejects int
	// PostingEntriesRead counts posting/doc entries returned by low-level index lookups.
	PostingEntriesRead int
	// CandidateDocs is the number of documents materialized into the final hit set before top-k truncation.
	CandidateDocs int
	// MatchedDocs is the number of documents matched by the executed query after query evaluation.
	MatchedDocs int
	// ReturnedDocs is the number of documents returned to the caller after maxResults truncation.
	ReturnedDocs int

	// Timings stores per-stage durations for this query execution.
	Timings map[string]time.Duration
}

type diagnosticsContextKey struct{}

type queryExecContext struct {
	mu sync.Mutex
	d  QueryDiagnostics
}

func ensureDiagnosticsContext(ctx context.Context) (context.Context, *queryExecContext) {
	if exec := diagnosticsFromContext(ctx); exec != nil {
		return ctx, exec
	}
	exec := &queryExecContext{d: QueryDiagnostics{Timings: make(map[string]time.Duration)}}
	return context.WithValue(ctx, diagnosticsContextKey{}, exec), exec
}

func diagnosticsFromContext(ctx context.Context) *queryExecContext {
	if ctx == nil {
		return nil
	}
	exec, _ := ctx.Value(diagnosticsContextKey{}).(*queryExecContext)
	return exec
}

func attachDiagnostics(ctx context.Context, res *SearchResult) *SearchResult {
	if res == nil {
		return nil
	}
	exec := diagnosticsFromContext(ctx)
	if exec == nil {
		return res
	}
	exec.setCandidateDocs(res.TotalResultsCount)
	exec.setMatchedDocs(res.TotalResultsCount)
	exec.setReturnedDocs(len(res.Results))
	res.Diagnostics = exec.snapshot()
	return res
}

func (q *queryExecContext) snapshot() *QueryDiagnostics {
	q.mu.Lock()
	defer q.mu.Unlock()
	copyD := q.d
	copyD.Timings = copyDurationMap(q.d.Timings)
	return &copyD
}

func copyDurationMap(src map[string]time.Duration) map[string]time.Duration {
	if len(src) == 0 {
		return map[string]time.Duration{}
	}
	out := make(map[string]time.Duration, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func (q *queryExecContext) setQueryTypeIfEmpty(v string) {
	if q == nil || v == "" {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.d.LogicalQueryType == "" {
		q.d.LogicalQueryType = v
	}
}

func (q *queryExecContext) setStrategy(v string) {
	if q == nil || v == "" {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.d.ExecutionStrategy == "" {
		q.d.ExecutionStrategy = v
	}
}

func (q *queryExecContext) setReasonIfEmpty(v string) {
	if q == nil || v == "" {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.d.StrategyReason == "" {
		q.d.StrategyReason = v
	}
}

func (q *queryExecContext) addTokens(n int) {
	if q == nil || n == 0 {
		return
	}
	q.mu.Lock()
	q.d.ProcessedTokens += n
	q.mu.Unlock()
}

func (q *queryExecContext) addFields(n int) {
	if q == nil || n == 0 {
		return
	}
	q.mu.Lock()
	q.d.FieldsVisited += n
	q.mu.Unlock()
}

func (q *queryExecContext) addKeys(n int) {
	if q == nil || n == 0 {
		return
	}
	q.mu.Lock()
	q.d.GeneratedKeys += n
	q.mu.Unlock()
}

func (q *queryExecContext) addIndexLookups(n int) {
	if q == nil || n == 0 {
		return
	}
	q.mu.Lock()
	q.d.IndexSearches += n
	q.mu.Unlock()
}

func (q *queryExecContext) addFilterCheck(missed bool) {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.d.FilterChecks++
	if missed {
		q.d.FilterRejects++
	}
	q.mu.Unlock()
}

func (q *queryExecContext) addPostingsRead(n int) {
	if q == nil || n == 0 {
		return
	}
	q.mu.Lock()
	q.d.PostingEntriesRead += n
	q.mu.Unlock()
}

func (q *queryExecContext) setCandidateDocs(n int) {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.d.CandidateDocs = n
	q.mu.Unlock()
}

func (q *queryExecContext) setMatchedDocs(n int) {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.d.MatchedDocs = n
	q.mu.Unlock()
}

func (q *queryExecContext) setReturnedDocs(n int) {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.d.ReturnedDocs = n
	q.mu.Unlock()
}

func (q *queryExecContext) addTiming(name string, d time.Duration) {
	if q == nil || name == "" {
		return
	}
	q.mu.Lock()
	q.d.Timings[name] = d
	q.mu.Unlock()
}

func queryTypeOf(q Query) string {
	switch t := q.(type) {
	case nil:
		return "empty"
	case TermQuery, *TermQuery:
		return "term"
	case PhraseQuery, *PhraseQuery:
		return "phrase"
	case PrefixQuery, *PrefixQuery:
		return "prefix"
	case *BooleanQuery:
		if t == nil || len(t.Clauses) == 0 {
			return "empty"
		}
		return "boolean"
	default:
		return "unsupported"
	}
}
