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

### 3) Snapshots

Index and filter snapshots are always stored in separate files.

Flow 1 (recommended): manual codec registration via `init()` + split files. If you enable scorer-aware ranking, prefer `SaveIndexSnapshotWithStats(...)` so restore can recover collection stats.

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

The old `SaveIndexSnapshot(...)` and `SaveMultiIndexSnapshot(...)` wrappers are still available for backward compatibility, but they do not persist `CollectionStats`. Use `SaveIndexSnapshotWithStats(...)` / `SaveMultiIndexSnapshotWithStats(...)` for scorer-aware restore.

Flow 2: ready-to-use built-in codecs and filters is now in examples:
- `examples/client-library/snapshot-save-files/main.go`
- `examples/client-library/snapshot-import-files/main.go`

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

All regular search methods already return structured per-request diagnostics via `SearchResult.Diagnostics`.

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
res, _ := engine.SearchDocuments(context.Background(), "postgres wal checkpoint", 10)

fmt.Println(res.Diagnostics.LogicalQueryType)
fmt.Println(res.Diagnostics.ExecutionStrategy)
fmt.Println(res.Diagnostics.StrategySkipReason)
fmt.Println(res.Diagnostics.Timings["total"])
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

	res, err := engine.SearchDocuments(context.Background(), "postgres checkpoint", 10)
	stats.ObserveSearch("postgres checkpoint", res.Diagnostics, err)

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

## Run main app (local testing via config)

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

Snapshot fields (`fts.snapshot`):

- `enabled`: enable snapshot persistence flow in CLI prod mode.
- `path`: base path used to derive split files when explicit paths are not set (`*.index.*` and `*.filter.*`).
- `index_path`: optional explicit path for index snapshot file.
- `filter_path`: optional explicit path for filter snapshot file.
- `load_on_start`: if true and snapshot exists, load it and skip rebuild.
- `save_on_build`: if true, save snapshot after indexing finishes.
- `buffer_size`: writer buffer size used during save.
- `flush_threshold`: buffered flush threshold used by the built-in save helper.
- `sync_file`: fsync temp file before atomic rename.

## CLI modes

- `prod`:
  - runs engine with configurable pipeline and interactive CUI search,
  - if `fts.snapshot.enabled=true` and `load_on_start=true` and snapshot exists: loads snapshot and skips re-index,
  - otherwise indexes documents and (if `save_on_build=true`) persists snapshot atomically.
- `experiment`:
  - always indexes current input and prints memory/index stats,
  - does not run CUI snapshot restore flow.

## Bench CLI

Use `cmd/bench` when you want to compare index/scorer/pipeline combinations on a corpus with labeled queries.

In addition to indexing time and search quality metrics, `cmd/bench` now also reports aggregated search diagnostics such as strategy distribution, diagnostics timings, posting reads, and index lookups.

Example:

```bash
go run ./cmd/bench \
  -dump ./data/enwiki-latest-abstract.xml.gz \
  -ground-truth ./internal/bench/testdata/queries.sample.json \
  -index slicedradix \
  -lang en \
  -field abstract \
  -scorer bm25 \
  -k 10
```

Useful flags:

- `-index`: `radix|slicedradix|hamt|hamtpointered`
- `-lang`: `en|ru|multi|none`
- `-field`: `abstract|extract|title`
- `-scorer`: `simple|bm25|tfidf`
- `-k`: top-k used for `nDCG` and `Recall`
- `-limit`: cap the number of indexed documents for quick experiments
- `-worst`: print worst queries by `nDCG`

### Benchmark baselines

For fair before/after comparisons keep these flags unchanged between runs:

- `-limit`
- `-index`
- `-lang`
- `-field`
- `-scorer`
- `-k`

Engine microbench:

```bash
go test -run '^$' -bench . -benchmem -count=5 ./pkg/fts | tee before-engine.txt
```

Index microbench:

```bash
go test -run '^$' -bench . -benchmem -count=5 \
  ./pkg/index/radix \
  ./pkg/index/slicedradix \
  ./pkg/index/hamt \
  ./pkg/index/hamtpointered | tee before-indexes.txt
```

Full microbench run:

```bash
go test -run '^$' -bench . -benchmem -count=5 \
  ./pkg/fts \
  ./pkg/index/radix \
  ./pkg/index/slicedradix \
  ./pkg/index/hamt \
  ./pkg/index/hamtpointered | tee before-micro-all.txt
```

Fast end-to-end bench on a local dump, for example `./data/enwiki-latest-abstract1.xml.gz`:

```bash
go run ./cmd/bench \
  -dump ./data/enwiki-latest-abstract1.xml.gz \
  -ground-truth ./internal/bench/testdata/queries.sample.json \
  -index slicedradix \
  -lang en \
  -field abstract \
  -scorer bm25 \
  -k 10 \
  -limit 5000 \
  -worst 5 | tee before-e2e-fast.txt
```

Medium end-to-end bench:

```bash
go run ./cmd/bench \
  -dump ./data/enwiki-latest-abstract1.xml.gz \
  -ground-truth ./internal/bench/testdata/queries.sample.json \
  -index slicedradix \
  -lang en \
  -field abstract \
  -scorer bm25 \
  -k 10 \
  -limit 20000 \
  -worst 10 | tee before-e2e-medium.txt
```

To compare index implementations on the same corpus, rerun `cmd/bench` with the same flags and vary only `-index`:

```bash
go run ./cmd/bench \
  -dump ./data/enwiki-latest-abstract1.xml.gz \
  -ground-truth ./internal/bench/testdata/queries.sample.json \
  -index radix \
  -lang en \
  -field abstract \
  -scorer bm25 \
  -k 10 \
  -limit 10000 \
  -worst 5 | tee before-radix.txt
```

```bash
go run ./cmd/bench \
  -dump ./data/enwiki-latest-abstract1.xml.gz \
  -ground-truth ./internal/bench/testdata/queries.sample.json \
  -index slicedradix \
  -lang en \
  -field abstract \
  -scorer bm25 \
  -k 10 \
  -limit 10000 \
  -worst 5 | tee before-slicedradix.txt
```

```bash
go run ./cmd/bench \
  -dump ./data/enwiki-latest-abstract1.xml.gz \
  -ground-truth ./internal/bench/testdata/queries.sample.json \
  -index hamt \
  -lang en \
  -field abstract \
  -scorer bm25 \
  -k 10 \
  -limit 10000 \
  -worst 5 | tee before-hamt.txt
```

```bash
go run ./cmd/bench \
  -dump ./data/enwiki-latest-abstract1.xml.gz \
  -ground-truth ./internal/bench/testdata/queries.sample.json \
  -index hamtpointered \
  -lang en \
  -field abstract \
  -scorer bm25 \
  -k 10 \
  -limit 10000 \
  -worst 5 | tee before-hamtpointered.txt
```

Recommended minimum baseline before a feature branch:

1. Run the engine microbench.
2. Run the index microbench.
3. Run the fast end-to-end bench.

After the change, rerun the same commands and save results into `after-*` files.

Compare these outputs:

- microbench: `ns/op`, `B/op`, `allocs/op`
- `cmd/bench`: indexing duration, latency `p50/p95/p99`, `diag.total`, `diag.search_tokens`, `avg postings`, `avg index_lookups`, strategy distribution, `nDCG`, `MRR`, `Recall`

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
