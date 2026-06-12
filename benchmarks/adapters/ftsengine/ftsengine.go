package ftsengine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/ftspersist"
	"github.com/dariasmyr/fts-engine/pkg/ftspreset"
	"github.com/dariasmyr/fts-engine/pkg/keygen"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/harness"
)

type Config struct {
	Index       string
	Scorer      string
	Lang        string
	Filter      string
	Persist     string
	Diagnostics bool
}

type Adapter struct {
	cfg        Config
	svc        *fts.Service
	workDir    string
	indexBytes int64

	mu    sync.Mutex
	extra extras
}

type extras struct {
	SearchesObserved       int
	PostingEntriesRead     int
	IndexSearches          int
	WANDUsed               int
	Strategies             map[string]int
	WANDSkipReasons        map[string]int
	StrategySkipReasons    map[string]int
	LogicalQueryTypes      map[string]int
	DiagnosticsEnabledRuns int
}

func New(cfg Config) *Adapter {
	return &Adapter{cfg: cfg.withDefaults()}
}

func (c Config) withDefaults() Config {
	if c.Index == "" {
		c.Index = "slicedradix"
	}
	if c.Scorer == "" {
		c.Scorer = "bm25"
	}
	if c.Lang == "" {
		c.Lang = "en"
	}
	if c.Filter == "" {
		c.Filter = "none"
	}
	if c.Persist == "" {
		c.Persist = "none"
	}
	return c
}

func (a *Adapter) Name() string { return "fts-engine" }

func (a *Adapter) Open(_ context.Context, dir string) error {
	index, err := ftsbuiltin.BuildIndex(a.cfg.Index)
	if err != nil {
		return fmt.Errorf("build index: %w", err)
	}
	flt, err := ftsbuiltin.BuildFilter(a.cfg.Filter, defaultFilterOptions())
	if err != nil {
		return fmt.Errorf("build filter: %w", err)
	}
	opts := make([]fts.Option, 0, 3)
	if presetOpt, err := selectPreset(a.cfg.Lang); err != nil {
		return err
	} else if presetOpt != nil {
		opts = append(opts, presetOpt)
	}
	if scorerOpt, err := selectScorer(a.cfg.Scorer); err != nil {
		return err
	} else if scorerOpt != nil {
		opts = append(opts, scorerOpt)
	}
	if flt != nil {
		opts = append(opts, fts.WithFilter(flt))
	}

	a.svc = fts.New(index, keygen.Word, opts...)
	a.workDir = dir
	a.indexBytes = 0
	a.extra = extras{
		Strategies:          make(map[string]int),
		WANDSkipReasons:     make(map[string]int),
		StrategySkipReasons: make(map[string]int),
		LogicalQueryTypes:   make(map[string]int),
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("prepare work dir: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create work dir: %w", err)
	}
	return nil
}

func (a *Adapter) Index(ctx context.Context, docs []harness.Document) error {
	for _, doc := range docs {
		if err := a.svc.Index(ctx, fts.Document{
			ID: fts.DocID(doc.ID),
			Fields: map[string]fts.Field{
				fts.DefaultField: {Value: doc.Body},
			},
		}); err != nil {
			return fmt.Errorf("index %q: %w", doc.ID, err)
		}
	}
	return nil
}

func (a *Adapter) Commit(ctx context.Context) error {
	_ = ctx
	switch a.cfg.Persist {
	case "", "none":
		a.indexBytes = 0
		return nil
	case "snapshot":
		return a.saveSnapshot()
	case "segment":
		return a.saveSegment()
	default:
		return fmt.Errorf("unsupported persist mode %q", a.cfg.Persist)
	}
}

func (a *Adapter) Search(ctx context.Context, q harness.Query) ([]harness.SearchHit, error) {
	if a.cfg.Diagnostics {
		ctx = fts.WithDiagnostics(ctx)
	}
	res, err := a.svc.SearchPlainText(ctx, q.Text, q.K)
	if err != nil {
		return nil, err
	}
	a.observeDiagnostics(res.Diagnostics)
	hits := make([]harness.SearchHit, 0, len(res.Results))
	for _, item := range res.Results {
		hits = append(hits, harness.SearchHit{DocID: string(item.ID), Score: item.Score})
	}
	return hits, nil
}

func (a *Adapter) IndexSizeBytes() (int64, error) { return a.indexBytes, nil }

func (a *Adapter) Close() error {
	a.svc = nil
	a.workDir = ""
	a.indexBytes = 0
	return nil
}

func (a *Adapter) BenchmarkMetadata() map[string]any {
	return map[string]any{
		"index":       a.cfg.Index,
		"scorer":      a.cfg.Scorer,
		"lang":        a.cfg.Lang,
		"filter":      a.cfg.Filter,
		"persist":     a.cfg.Persist,
		"diagnostics": a.cfg.Diagnostics,
	}
}

func (a *Adapter) BenchmarkExtras() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.cfg.Diagnostics {
		return map[string]any{"diagnostics_enabled": false}
	}
	return map[string]any{
		"diagnostics_enabled":   true,
		"searches_observed":     a.extra.SearchesObserved,
		"diagnostics_runs":      a.extra.DiagnosticsEnabledRuns,
		"posting_entries_read":  a.extra.PostingEntriesRead,
		"index_searches":        a.extra.IndexSearches,
		"wand_used":             a.extra.WANDUsed,
		"strategies":            copyIntMap(a.extra.Strategies),
		"wand_skip_reasons":     copyIntMap(a.extra.WANDSkipReasons),
		"strategy_skip_reasons": copyIntMap(a.extra.StrategySkipReasons),
		"logical_query_types":   copyIntMap(a.extra.LogicalQueryTypes),
	}
}

func (a *Adapter) saveSnapshot() error {
	if err := ftsbuiltin.RegisterSnapshotCodecs(); err != nil {
		return fmt.Errorf("register snapshot codecs: %w", err)
	}
	paths := ftspersist.SnapshotPaths{IndexPath: filepath.Join(a.workDir, "default.index.fidx")}
	if a.cfg.Filter != "none" && a.cfg.Filter != "" {
		paths.FilterPath = filepath.Join(a.workDir, "default.filter.fidx")
	}
	if err := ftspersist.SaveSnapshot(paths, a.svc, a.cfg.Index, filterCodecName(a.cfg.Filter), ftspersist.SaveOptions{}); err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	size, err := pathSize(a.workDir)
	if err != nil {
		return fmt.Errorf("snapshot size: %w", err)
	}
	a.indexBytes = size
	return nil
}

func (a *Adapter) saveSegment() error {
	if err := ftsbuiltin.RegisterSnapshotCodecs(); err != nil {
		return fmt.Errorf("register snapshot codecs: %w", err)
	}
	segmentDir := filepath.Join(a.workDir, "segment")
	if err := os.MkdirAll(segmentDir, 0o755); err != nil {
		return fmt.Errorf("create segment dir: %w", err)
	}
	if err := ftspersist.SaveSegment(ftspersist.SegmentPaths{Dir: segmentDir}, a.svc, filterCodecName(a.cfg.Filter), ftspersist.SaveOptions{}); err != nil {
		return fmt.Errorf("save segment: %w", err)
	}
	size, err := pathSize(segmentDir)
	if err != nil {
		return fmt.Errorf("segment size: %w", err)
	}
	a.indexBytes = size
	return nil
}

func (a *Adapter) observeDiagnostics(diag *fts.QueryDiagnostics) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.extra.SearchesObserved++
	if diag == nil {
		return
	}
	a.extra.DiagnosticsEnabledRuns++
	a.extra.PostingEntriesRead += diag.PostingEntriesRead
	a.extra.IndexSearches += diag.IndexSearches
	if diag.ExecutionStrategy != "" {
		a.extra.Strategies[diag.ExecutionStrategy]++
	}
	if diag.StrategySkipReason != "" {
		a.extra.StrategySkipReasons[diag.StrategySkipReason]++
	}
	if diag.LogicalQueryType != "" {
		a.extra.LogicalQueryTypes[diag.LogicalQueryType]++
	}
	if diag.Boolean != nil {
		if diag.Boolean.WAND.Used {
			a.extra.WANDUsed++
		}
		if diag.Boolean.WAND.SkipReason != "" {
			a.extra.WANDSkipReasons[diag.Boolean.WAND.SkipReason]++
		}
	}
}

func selectPreset(lang string) (fts.Option, error) {
	switch lang {
	case "", "none":
		return nil, nil
	case "en":
		return ftspreset.English(), nil
	case "ru":
		return ftspreset.Russian(), nil
	case "multi":
		return ftspreset.Multilingual(), nil
	default:
		return nil, fmt.Errorf("unknown lang preset %q", lang)
	}
}

func selectScorer(kind string) (fts.Option, error) {
	switch kind {
	case "", "none":
		return nil, nil
	case "bm25":
		return fts.WithScorer(fts.BM25()), nil
	case "tfidf":
		return fts.WithScorer(fts.TFIDF()), nil
	default:
		return nil, fmt.Errorf("unknown scorer %q", kind)
	}
}

func defaultFilterOptions() ftsbuiltin.FilterOptions {
	return ftsbuiltin.FilterOptions{
		BloomExpectedItems:  1_000_000,
		BloomBitsPerItem:    10,
		BloomK:              7,
		CuckooBucketCount:   1 << 18,
		CuckooBucketSize:    4,
		CuckooMaxKicks:      200,
		RibbonExpectedItems: 1_000_000,
		RibbonExtraCells:    250_000,
		RibbonWindowSize:    24,
		RibbonSeed:          7,
		RibbonMaxAttempts:   8,
	}
}

func filterCodecName(name string) string {
	if name == "" || name == "none" {
		return ""
	}
	return name
}

func pathSize(path string) (int64, error) {
	var total int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

func copyIntMap(src map[string]int) map[string]int {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]int, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}
