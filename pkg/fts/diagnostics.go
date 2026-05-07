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
	// StrategySkipReason stores the first useful reason why an earlier strategy or fast path was skipped.
	StrategySkipReason string
	// Boolean stores strategy-specific diagnostics for boolean execution paths.
	Boolean *BooleanDiagnostics

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
	Timings QueryTimings
}

type QueryTimings struct {
	Preprocess   time.Duration
	SearchTokens time.Duration
	Total        time.Duration

	set queryTimingMask
}

type queryTimingMask uint8

const (
	queryTimingPreprocess queryTimingMask = 1 << iota
	queryTimingSearchTokens
	queryTimingTotal
)

func (t QueryTimings) HasPreprocess() bool {
	return t.set&queryTimingPreprocess != 0
}

func (t QueryTimings) HasSearchTokens() bool {
	return t.set&queryTimingSearchTokens != 0
}

func (t QueryTimings) HasTotal() bool {
	return t.set&queryTimingTotal != 0
}

type diagnosticsEnabledContextKey struct{}

func WithDiagnostics(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, diagnosticsEnabledContextKey{}, true)
}

func diagnosticsRequested(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	enabled, _ := ctx.Value(diagnosticsEnabledContextKey{}).(bool)
	return enabled
}

type BooleanDiagnostics struct {
	FastPathSkips       int
	FastPathSkipReasons []string
	WAND                WANDDiagnostics
	AndFast             AndFastDiagnostics
}

type WANDDiagnostics struct {
	Eligible           bool
	Used               bool
	SkipReason         string
	ClauseCount        int
	PostingsPerClause  []int
	PostingsConsidered int
	CandidateDocs      int
	HeapSize           int
	TopK               int
	FinalTheta         float64
}

type AndFastDiagnostics struct {
	Eligible        bool
	Used            bool
	SkipReason      string
	DriverGroupSize int
	OtherGroupCount int
	CandidateDocs   int
	UsedSortMerge   bool
	BuiltLookupMaps bool
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
	if !diagnosticsRequested(ctx) {
		return ctx, nil
	}
	exec := &queryExecContext{}
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
	copyD.Boolean = copyBooleanDiagnostics(q.d.Boolean)
	return &copyD
}

func copyBooleanDiagnostics(src *BooleanDiagnostics) *BooleanDiagnostics {
	if src == nil {
		return nil
	}
	out := *src
	out.FastPathSkipReasons = append([]string(nil), src.FastPathSkipReasons...)
	out.WAND.PostingsPerClause = append([]int(nil), src.WAND.PostingsPerClause...)
	return &out
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

func (q *queryExecContext) setSkipReasonIfEmpty(v string) {
	if q == nil || v == "" {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.d.StrategySkipReason == "" {
		q.d.StrategySkipReason = v
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

func (q *queryExecContext) setPreprocessTiming(d time.Duration) {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.d.Timings.Preprocess = d
	q.d.Timings.set |= queryTimingPreprocess
	q.mu.Unlock()
}

func (q *queryExecContext) setSearchTokensTiming(d time.Duration) {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.d.Timings.SearchTokens = d
	q.d.Timings.set |= queryTimingSearchTokens
	q.mu.Unlock()
}

func (q *queryExecContext) setTotalTiming(d time.Duration) {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.d.Timings.Total = d
	q.d.Timings.set |= queryTimingTotal
	q.mu.Unlock()
}

func (q *queryExecContext) startTimer() time.Time {
	if q == nil {
		return time.Time{}
	}
	return time.Now()
}

func (q *queryExecContext) observePreprocess(start time.Time) {
	if q == nil || start.IsZero() {
		return
	}
	q.setPreprocessTiming(time.Since(start))
}

func (q *queryExecContext) observeSearchTokens(start time.Time) {
	if q == nil || start.IsZero() {
		return
	}
	q.setSearchTokensTiming(time.Since(start))
}

func (q *queryExecContext) observeTotal(start time.Time) {
	if q == nil || start.IsZero() {
		return
	}
	q.setTotalTiming(time.Since(start))
}

func (q *queryExecContext) updateBooleanDiagnostics(fn func(*BooleanDiagnostics)) {
	if q == nil || fn == nil {
		return
	}
	q.mu.Lock()
	if q.d.Boolean == nil {
		q.d.Boolean = &BooleanDiagnostics{}
	}
	fn(q.d.Boolean)
	q.mu.Unlock()
}

func (q *queryExecContext) recordFastPathSkip(reason string) {
	q.updateBooleanDiagnostics(func(b *BooleanDiagnostics) {
		b.FastPathSkips++
		if reason != "" {
			b.FastPathSkipReasons = append(b.FastPathSkipReasons, reason)
		}
	})
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
