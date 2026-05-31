# Fast Turtle Search Engine

Reusable full-text search engine in Go with configurable indexes, filters, stemming pipeline, and snapshot support.

<p align="center"><img src="docs/logo.png" alt="Logo" width="50%"></p>

### Demo Cast

![Demo](docs/demo.gif)

## What this repository provides

- Public library API in `pkg/fts`.
- Public index implementations in `pkg/index/*`:
  - `radix` (exact + positional)
  - `slicedradix` (exact + positional + prefix)
  - `hamt` (exact + positional)
  - `hamtpointered` (exact + positional)
- Public text processing pipeline in `pkg/textproc`.
- Public key generators in `pkg/keygen`.
- Public probabilistic filters in `pkg/filter`.
- Public diagnostics observer in `pkg/ftsstats`.
- CLI entrypoint in `cmd/fts` with:
  - `prod` mode (run with configurable filters and interactive CUI)
  - `experiment` mode (collect indexing metrics)
- Benchmark/evaluation CLI in `cmd/bench` for indexing throughput, latency, `nDCG`, `MRR`, and `Recall`.

## Library usage

### 1) Install

```bash
go get github.com/dariasmyr/fts-engine@latest
```

### 2) Quickstart

```go
package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/radix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func main() {
	engine := fts.New(radix.New(), keygen.Word)

	_ = engine.IndexDocument(context.Background(), "doc-1", "Wikipedia: Rosa is a French hotel barge")
	res, _ := engine.SearchDocuments(context.Background(), "french hotel", 10)

	fmt.Println(res.TotalResultsCount)
}
```

### 3) Persistence Modes

The project currently exposes two different persistence models:

- `snapshot`: mutable persistence for restoring an index and continuing writes
- `segment`: immutable persistence for restoring a sealed read-only search index

`mmap` belongs only to the `segment` model. It is a way to open raw segment files, not a separate persistence format.

#### Snapshot Files (`pkg/fts`)

Use snapshot files when you want to restore an index and continue indexing after process restart.

- writable after restore
- based on registered index/filter snapshot codecs
- best default when you need mutable runtime behavior

Manual codec registration via `init()` + split files. If you enable scorer-aware ranking, prefer `SaveIndexSnapshotWithStats(...)` so restore can recover collection stats.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func init() {
	_ = fts.RegisterIndexSnapshotCodec("slicedradix",
		func(index fts.Index, w io.Writer) error {
			s, ok := index.(fts.Serializable)
			if !ok {
				return fmt.Errorf("slicedradix: index does not implement Serializable")
			}
			return s.Serialize(w)
		},
		slicedradix.Load,
	)
}

func main() {
	svc := fts.New(slicedradix.New(), keygen.Word, fts.WithScorer(fts.BM25()))
	_ = svc.IndexDocument(context.Background(), "doc-1", "snapshot demo")
	idx, _ := svc.SnapshotComponents()
	stats := svc.SnapshotCollectionStats()

	// export to file
	idxOut, _ := os.Create("./data/segments/default.index.fidx")
	defer idxOut.Close()
	_ = fts.SaveIndexSnapshotWithStats(idxOut, "slicedradix", idx, stats)

	
	// open from file
	idxIn, _ := os.Open("./data/segments/default.index.fidx")
	defer idxIn.Close()
	loadedIndex, _ := fts.LoadIndexSnapshot(idxIn)
	restored := fts.New(
		loadedIndex.Index,
		keygen.Word,
		fts.WithScorer(fts.BM25()),
		fts.WithCollectionStatsSnapshot(loadedIndex.CollectionStats),
	)

	res, _ := restored.SearchDocuments(context.Background(), "snapshot", 10)
	fmt.Println(res.TotalResultsCount)
}
```

If you use scorer-aware restore, prefer `SaveIndexSnapshotWithStats(...)` / `SaveMultiIndexSnapshotWithStats(...)` so `CollectionStats` are preserved.

The current client-library file persistence examples under:

- `examples/client-library/snapshot-save-files/main.go`
- `examples/client-library/snapshot-load-files/main.go`
- `examples/client-library/snapshot-load-files-low-level/main.go`

demonstrate mutable `snapshot` export/restore for a service created with `fts.New(...)`, including both a high-level restore path and an explicit low-level restore path.

#### Segment Files (`pkg/segment`)

Use segments when you want to export a sealed read-only index for search.

- read-only after restore
- built from mutable indexes that implement `segment.Source`
- suitable for file-backed loading and raw segment `mmap`

The current client-library file persistence examples under:

- `examples/client-library/segment-save-files/main.go`
- `examples/client-library/segment-load-files/main.go`

demonstrate `segment` export/restore for a service created with `fts.New(...)`.

Format compatibility rules:

- snapshot files must be loaded with `fts.LoadIndexSnapshot(...)` or `fts.LoadMultiIndexSnapshot(...)`
- segment files must be loaded with `pkg/segment` APIs
- a snapshot file cannot be opened as a segment file
- a segment file cannot be loaded as a snapshot file

Example:

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftspersist"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func main() {
	svc := fts.New(slicedradix.New(), keygen.Word, fts.WithScorer(fts.BM25()))
	_ = svc.IndexDocument(context.Background(), "doc-1", "segment demo")

	if err := os.MkdirAll("./data/segments", 0o755); err != nil {
		panic(err)
	}

	if err := ftspersist.SaveSegment(ftspersist.SegmentPaths{Dir: "./data/segments/default"}, svc, "", ftspersist.SaveOptions{SyncFile: true}); err != nil {
		panic(err)
	}

	loaded, err := ftspersist.LoadSegment(ftspersist.SegmentPaths{Dir: "./data/segments/default"}, keygen.Word, ftspersist.SegmentLoadOptions{Access: ftspersist.AccessFile}, fts.WithScorer(fts.BM25()))
	if err != nil {
		panic(err)
	}
	defer loaded.Close()

	res, _ := loaded.Service.SearchDocuments(context.Background(), "segment", 10)
	fmt.Println(res.TotalResultsCount)
}
```

Important:

- this restores a read-only search service
- the current client-library examples use a manifest-backed segment directory layout
- raw `segment.OpenFile(...)` is a lower-level API for opening raw segment files directly

#### `mmap` and Segments

Raw segment files can be opened with `segment.OpenFile(...)`.

- this is available only for raw segment files
- `segment.Reader` stays read-only
- current `segment-*` examples use ordinary file loading, not `mmap`

Today, `mmap` is a low-level `pkg/segment` API rather than the main persistence flow used by the CLI.

### 4) Custom pipeline and language presets

Default preset shortcut:

```go
engine := fts.New(radix.New(), keygen.Word, ftspreset.English())
```

Available presets:

- `textproc.DefaultEnglishPipeline()`
- `textproc.DefaultRussianPipeline()`
- `textproc.DefaultMultilingualPipeline()`
- `ftspreset.English()` / `ftspreset.Russian()` / `ftspreset.Multilingual()`

Custom pipeline:

```go
pipe := textproc.NewPipeline(
	textproc.AlnumTokenizer{},
	textproc.LowercaseFilter{},
	textproc.MinLengthOrNumericFilter{MinLength: 2},
	textproc.EnglishStopwordFilter{},
	textproc.EnglishStemFilter{},
)

engine := fts.New(radix.New(), keygen.Word, fts.WithPipeline(pipe))
```

### 5) Query types

String query parsing via `SearchDocuments(...)` supports:

- term query: `hotel`
- multiple terms as independent SHOULD clauses: `french hotel`
- phrase query: `"hotel barge"`
- required term: `+hotel`
- excluded term: `-market`
- field-scoped term: `title:hotel`
- field-scoped phrase: `title:"hotel barge"`
- prefix query: `bar*`
- grouped boolean clauses: `+(title:barack title:russia) -market`

Examples:

```go
// Two independent SHOULD terms.
res, _ := engine.SearchDocuments(context.Background(), "french hotel", 10)

// Exact phrase search.
res, _ = engine.SearchDocuments(context.Background(), `"french hotel"`, 10)

// Boolean syntax with required, excluded, and grouped clauses.
res, _ = engine.SearchDocuments(context.Background(), `+(title:barack title:french) -market`, 10)
```

If you want to build queries programmatically, use the AST API:

```go
q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
	fts.MustClause(fts.TermQuery{Term: "hotel"}),
	fts.ShouldClause(fts.PhraseQuery{Phrase: "french barge"}),
	fts.MustNotClause(fts.TermQuery{Term: "market"}),
}}

res, _ := engine.Search(context.Background(), q, 10)
fmt.Println(res.TotalResultsCount)
```

Field-scoped helpers are also available when you want to restrict search to one field without building the AST manually:

```go
res, _ := engine.SearchField(context.Background(), "title", "hotel", 10)
res, _ = engine.SearchPhraseField(context.Background(), "title", "hotel barge", 10)
res, _ = engine.SearchPhraseNearField(context.Background(), "body", "barack obama", 1, 10)
```

If you want to search across a specific subset of fields, use the `...Fields` variants:

```go
res, _ := engine.SearchFields(context.Background(), []string{"title", "body"}, "hotel", 10)
res, _ = engine.SearchPhraseFields(context.Background(), []string{"title", "body"}, "hotel barge", 10)
res, _ = engine.SearchPhraseNearFields(context.Background(), []string{"title", "body"}, "barack obama", 1, 10)
```

If you want different subqueries for different fields without building the AST manually, use field clauses:

```go
res, _ := engine.SearchFieldClauses(context.Background(), []fts.FieldQueryClause{
	fts.MustFieldQuery("title", "barack"),
	fts.MustFieldQuery("body", `"french hotel"`),
	fts.MustNotFieldQuery("body", "market"),
}, 10)
```

Prefix queries require an index that implements `fts.PrefixIndex`.
Among the built-in public indexes, `slicedradix` currently supports prefix search.

### 6) Diagnostics and aggregated stats

Per-request diagnostics are opt-in. Regular search methods return `SearchResult.Diagnostics == nil` unless you enable diagnostics for the request context with `fts.WithDiagnostics(ctx)`.

Useful methods include:

- `Search(...)`
- `SearchDocuments(...)`
- `SearchField(...)`
- `SearchFields(...)`
- `SearchFieldClauses(...)`
- `SearchPhrase(...)`
- `SearchPhraseNear(...)`

Example:

```go
ctx := fts.WithDiagnostics(context.Background())
res, _ := engine.SearchDocuments(ctx, "postgres wal checkpoint", 10)

fmt.Println(res.Diagnostics.LogicalQueryType)
fmt.Println(res.Diagnostics.ExecutionStrategy)
fmt.Println(res.Diagnostics.StrategySkipReason)
fmt.Println(res.Diagnostics.Timings.Total)
fmt.Println(res.Diagnostics.PostingEntriesRead)
```

If you want aggregated observability across many requests, use `pkg/ftsstats`.

```go
package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsstats"
	"github.com/dariasmyr/fts-engine/pkg/index/radix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func main() {
	engine := fts.New(radix.New(), keygen.Word)
	stats := ftsstats.NewSearchStats(64)

	_ = engine.IndexDocument(context.Background(), "doc-1", "postgres wal checkpoint tuning")
	_ = engine.IndexDocument(context.Background(), "doc-2", "checkpoint and recovery internals")

	ctx := fts.WithDiagnostics(context.Background())
	res, err := engine.SearchDocuments(ctx, "postgres checkpoint", 10)
	stats.ObserveResult("postgres checkpoint", res, err)

	snap := stats.Snapshot()
	for strategy, st := range snap.ByStrategy {
		fmt.Println(strategy, st.Count, st.AvgDuration(), st.MaxDuration)
	}

	for _, ev := range stats.Recent(5) {
		fmt.Println(ev.QueryHash, ev.ExecutionStrategy, ev.TotalDuration)
	}
}
```

`pkg/ftsstats` is the recommended programmatic surface for agents and library consumers that need:

- recent search events without storing raw query text by default;
- aggregated stats by execution strategy;
- structured observability without depending on `cmd/fts` or any HTTP/debug transport layer.

`ObserveResult(...)` is tolerant: it always records base request/error information, uses `SearchResult` fields like `TotalResultsCount` and returned hit count even when diagnostics are disabled, and fills diagnostics-dependent fields only when `res.Diagnostics` is non-nil.

## Usage Modes

The repository currently supports these top-level usage patterns:

- library mode in memory: create `fts.New(...)` or `fts.NewMultiField(...)` and manage indexing/search yourself
- library mode with mutable snapshots: use `pkg/fts` snapshot save/load helpers
- library mode with immutable segments: use `pkg/segment` export/load helpers
- app mode via config: run `cmd/fts` and let the repository app manage startup/build persistence

## Run Main App

Use this only when you want to test the repository app itself (`cmd/fts`), not when embedding the library into your service.

Download the Wikipedia dump from:

`https://archive.org/download/enwiki-20210820`

1) Create config from template:

```bash
cp ./config/config_local_example.yaml ./config/config_local.yaml
```

2) Run with config:

```bash
go run ./cmd/fts --config=./config/config_local.yaml
```

Important config fields:

```yaml
fts:
  engine: "trie"
  index: "radix"       # radix|slicedradix|hamt|hamtpointered
  keygen: "word"
  scorer: "none"       # none|bm25|tfidf
  filter: "none"       # none|bloom|cuckoo|ribbon
  snapshot:
    enabled: true
    path: "./data/segments/default.fidx"
    index_path: "./data/segments/local.index.fidx"
    filter_path: "./data/segments/local.filter.fidx"
    load_on_start: true
    save_on_build: true
    buffer_size: 1048576
    flush_threshold: 262144
    sync_file: true
  bloom:
    expected_items: 1000000
    bits_per_item: 10
    k: 7
  cuckoo:
    bucket_count: 262144
    bucket_size: 4
    max_kicks: 500
  ribbon:
    expected_items: 1000000
    extra_cells: 250000
    window_size: 24      # 1..32
    seed: 0
    max_attempts: 5
  pipeline:
    lowercase: true
    stopwords_en: true
    stopwords_ru: false
    stem_en: true
    stem_ru: false
    min_length: 3
mode:
  type: "prod"        # prod|experiment
```

Persistence fields in the current CLI config (`fts.snapshot`):

- `enabled`: enable persistence flow in CLI prod mode.
- `path`: base path used by the current CLI persistence logic.
- `index_path`: optional explicit path for mutable index snapshot file.
- `filter_path`: optional explicit path for filter snapshot file.
- `load_on_start`: if true and persisted state exists, load it and skip rebuild.
- `save_on_build`: if true, save persisted state after indexing finishes.
- `buffer_size`: writer buffer size used during save.
- `flush_threshold`: buffered flush threshold used by the built-in save helper.
- `sync_file`: fsync temp file before atomic rename.

## CLI modes

- `prod`:
  - runs engine with configurable pipeline and interactive CUI search,
  - if `fts.snapshot.enabled=true` and `load_on_start=true` and persisted state exists: loads it and skips re-index,
  - otherwise indexes documents and (if `save_on_build=true`) persists state atomically.
- `experiment`:
  - always indexes current input and prints memory/index stats,
  - does not run CUI snapshot restore flow.

## Benchmarks

This repository uses three benchmark types:

- end-to-end bench: `go run ./cmd/bench ...`
- engine microbench: `go test -run '^$' -bench . ./pkg/fts`
- index microbench: `go test -run '^$' -bench . ./pkg/index/...`

Use `tee file.txt` when you want to both see benchmark output in the terminal and save the same output into a file for before/after comparison.

### 1) End-to-End Bench

Use `cmd/bench` when you want to compare index, scorer, and pipeline combinations on a corpus with labeled queries.

It measures:

- indexing duration
- search latency `p50/p95/p99`
- quality metrics: `nDCG`, `MRR`, `Recall`
- optional diagnostics: `diag.total`, `diag.search_tokens`, strategy distribution, zero-result counts, WAND/fallback distributions, posting reads, index lookups

Common flags:

- `-dump`: wiki dump path
- `-ground-truth`: labeled query set path
- `-index`: `radix|slicedradix|hamt|hamtpointered`
- `-lang`: `en|ru|multi|none`
- `-field`: `abstract|extract|title`
- `-scorer`: `none|bm25|tfidf`
- `-k`: top-k used for `nDCG` and `Recall`
- `-limit`: cap the number of indexed documents for quick experiments
- `-worst`: print worst queries by `nDCG`; with diagnostics enabled also print worst queries by `postings_read`
- `-diagnostics`: enable per-query `SearchResult.Diagnostics`
- `-observer`: enable `ftsstats.SearchStats` aggregation during the benchmark run
- `-warmup`: run N warmup searches before measured runs; warmup does not affect reported metrics
- `-repeat`: repeat the full query set N times for measured runs
- `-shuffle`: shuffle query order for warmup and each measured repeat with a fixed seed

Local workloads checked in under `internal/bench/testdata/`:

- `queries.abstract1.title.json`: tiny sanity-check workload for `-field title`
- `queries.abstract1.abstract.json`: tiny sanity-check workload for `-field abstract`
- `queries.abstract1.abstract.50.json`: curated 50-query `abstract` workload for repeated latency comparisons

Steady-state example:

```bash
go run ./cmd/bench \
  -dump ./data/enwiki-20210820-abstract1.xml.gz \
  -ground-truth ./internal/bench/testdata/queries.abstract1.abstract.50.json \
  -index radix \
  -lang en \
  -field abstract \
  -scorer none \
  -k 10 \
  -warmup 50 \
  -repeat 20 \
  -shuffle true \
  -diagnostics false \
  -observer false | tee before-e2e.txt
```

Cold-run example:

```bash
go run ./cmd/bench \
  -dump ./data/enwiki-20210820-abstract1.xml.gz \
  -ground-truth ./internal/bench/testdata/queries.abstract1.abstract.50.json \
  -index radix \
  -lang en \
  -field abstract \
  -scorer none \
  -k 10 \
  -warmup 0 \
  -repeat 1 \
  -shuffle false \
  -diagnostics false \
  -observer false | tee before-e2e-cold.txt
```

This keeps the first measured pass cold inside a fresh `cmd/bench` process. If you rerun the command, treat each process start as a separate cold run.

To compare instrumentation overhead, keep all other flags the same and vary only:

- `-diagnostics`
- `-observer`

To compare index implementations on the same corpus, keep all other flags the same and vary only:

- `-index`

Current latency numbers are measured around `SearchDocuments(...)`. `-observer` still exercises the observer path and prints observer summary, but observer work is not included in the reported `latency p50/p95/p99` yet. The text report uses `p50/p95/p99` as the main latency view; the diagnostics-heavy breakdown focuses on strategies, postings, WAND usage, fallback reasons, and zero-result counts.

### 2) Engine Microbench

Use the engine microbench when you want to measure the `pkg/fts` search engine in isolation from the end-to-end CLI flow.

It typically measures:

- `ns/op`
- `B/op`
- `allocs/op`

Useful `go test` flags:

- `-bench .`: run all benchmarks in the package
- `-benchmem`: print allocation stats
- `-count=5`: repeat the benchmark 5 times
- `-run '^$'`: skip regular unit tests

Example:

```bash
go test -run '^$' -bench . -benchmem -count=5 ./pkg/fts | tee before-engine.txt
```

### 3) Index Microbench

Use the index microbench when you want to measure low-level index operations in the concrete index implementations.

It typically measures:

- exact lookup cost
- positional lookup cost
- insert cost
- positional insert cost
- allocations per operation

It uses the same `go test` flags as the engine microbench.

Example across all current public indexes:

```bash
go test -run '^$' -bench . -benchmem -count=5 \
  ./pkg/index/radix \
  ./pkg/index/slicedradix \
  ./pkg/index/hamt \
  ./pkg/index/hamtpointered | tee before-indexes.txt
```

If you want one command that runs both microbench groups together, use:

```bash
go test -run '^$' -bench . -benchmem -count=5 \
  ./pkg/fts \
  ./pkg/index/radix \
  ./pkg/index/slicedradix \
  ./pkg/index/hamt \
  ./pkg/index/hamtpointered | tee before-micro-all.txt
```

### Benchmark Baselines

For fair before/after comparisons:

1. Run the same benchmark type before the change.
2. Save the output into `before-*` files.
3. Rerun the same commands after the change and save them into `after-*` files.
4. Compare like-for-like outputs only.

For end-to-end comparisons keep these flags unchanged between runs:

- `-limit`
- `-index`
- `-lang`
- `-field`
- `-scorer`
- `-k`
- `-warmup`
- `-repeat`
- `-shuffle`
- `-diagnostics`
- `-observer`

Recommended minimum baseline before a feature branch:

1. Run the engine microbench.
2. Run the index microbench if you touched index code.
3. Run the end-to-end bench on a representative local dump.

Compare these outputs:

- microbench: `ns/op`, `B/op`, `allocs/op`
- `cmd/bench`: indexing duration, latency `p50/p95/p99`, zero-result counts, `diag.total`, `diag.search_tokens`, `avg postings`, `avg index_lookups`, strategy distribution, WAND/fallback breakdowns, worst-by-postings, `nDCG`, `MRR`, `Recall`

## Ribbon filter usage

Ribbon is a static filter. In `fts` it is used via `BufferedStaticFilter`.

Preferred build API is stream-based (`BuildWithRetriesFromKeyStream`).

```go
expectedItems := uint32(1_000_000) // estimated unique keys
extraCells := uint32(250_000)
windowSize := uint32(16)
seed := uint64(0)
maxAttempts := uint32(5)

rf, _ := filter.NewRibbonFilter(
	expectedItems,
	extraCells,
	windowSize,
	seed,
)

stream := func(emit func([]byte) bool) error {
	keys := []string{"alpha", "hotel", "market"}
	for _, key := range keys {
		if !emit([]byte(key)) {
			break
		}
	}
	return nil
}

_ = rf.BuildWithRetriesFromKeyStream(stream, maxAttempts)

out, _ := os.Create("./data/segments/ribbon.filter.fidx")
defer out.Close()
_ = rf.Serialize(out)
```

If your keys come from files, add a thin adapter in client code that converts file parsing to stream emission.

Minimal parser adapter example (line-by-line keys):

```go
func parseKeysFile(path string, emit func([]byte) bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		key := strings.TrimSpace(s.Text())
		if key == "" {
			continue
		}
		if !emit([]byte(key)) {
			break
		}
	}

	return s.Err()
}
```

Load ribbon filter from file:

```go
in, _ := os.Open("./data/segments/ribbon.filter.fidx")
defer in.Close()

ribbonFilter, _ := filter.LoadRibbonFilter(in)

fmt.Println(ribbonFilter.Contains([]byte("market")))
```

### Standalone filter `Contains` with normalization

Use this when you store normalized keys in filter and later want to check a raw user word.

Example: indexed key is `beauty`, user enters `beautiful`.
With stemming, both become `beauti`, so normalized check returns `true`.

```go
pipe := textproc.NewPipeline(
	textproc.AlnumTokenizer{},
	textproc.LowercaseFilter{},
	textproc.EnglishStemFilter{},
)

indexedTerms := []string{"beauty", "hotel"}
normalizedKeys := make([]string, 0, len(indexedTerms))
for _, term := range indexedTerms {
	keys, _ := fts.NormalizeToKeys(term, pipe, keygen.Word)
	normalizedKeys = append(normalizedKeys, keys...)
}

rf, _ := filter.NewRibbonFilter(uint32(len(normalizedKeys)), 32, 24, 0)
stream := func(emit func([]byte) bool) error {
	for _, key := range normalizedKeys {
		if !emit([]byte(key)) {
			break
		}
	}
	return nil
}

_ = rf.BuildWithRetriesFromKeyStream(stream, 5)

raw := rf.Contains([]byte("beautiful")) // false: filter stores normalized keys

normalized, _ := fts.ContainsNormalized(rf, "beautiful", pipe, keygen.Word)

fmt.Println("raw", raw, "normalized", normalized) // raw=false normalized=true
```

`ContainsNormalized` applies pipeline + keygen and checks all normalized keys via `Contains`.

## Tests

Run all tests:

```bash
go test ./...
```

Run only public packages:

```bash
go test ./pkg/...
```

Run microbenchmarks for the FTS engine and all current index implementations:

```bash
go test -run '^$' -bench . -benchmem ./pkg/fts ./pkg/index/radix ./pkg/index/slicedradix ./pkg/index/hamt ./pkg/index/hamtpointered
```
