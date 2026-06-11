# benchmarks

Cross-engine benchmark suite for the current `fts-engine` repository.

This directory is intentionally separate from the existing repository benchmark and evaluation tooling under `cmd/bench` and `internal/bench`. The existing tooling remains the engine's native evaluation path. The suite in `benchmarks/` is for cross-engine comparison.

## Iteration 0 Status

This document defines the MVP scope, baseline methodology, and default comparison settings for the benchmark suite.

## Goals

- Compare the current `fts-engine` implementation against other Go or external FTS engines.
- Keep the suite separate from library and application code.
- Produce repeatable benchmark runs with explicit engine configuration.
- Measure both search quality and operational performance.

## Non-Goals

- Replacing the existing `cmd/bench` command.
- Reusing the fork's benchmark code unchanged.
- Hiding analyzer or query-semantics differences between engines.
- Shipping per-engine tuning in the baseline comparison.

## Current Repository Constraints

The suite must target the current repository shape rather than the fork's API.

Current `fts-engine` integration points:

- service construction via `fts.New(...)`
- indexing via `svc.Index(ctx, fts.Document{...})`
- searching via `svc.SearchDocuments(...)`
- optional persistence via `pkg/ftspersist` snapshot and segment helpers

The suite must also respect that this repository already has:

- native quality and diagnostics tooling in `internal/bench`
- index variants `slicedradix` and `hamt`
- scorer options `none`, `bm25`, `tfidf`
- optional diagnostics such as strategy selection, postings read, and WAND usage

## MVP Scope

The first usable version of the suite will include:

- a separate Go module under `benchmarks/`
- a shared harness and report format
- adapters for `fts-engine`, `bleve`, and `bluge`
- synthetic and MS MARCO datasets
- JSON and plain-text table outputs

The first usable version does not require:

- `tantivy`
- `riot`
- per-engine tuning profiles
- fancy terminal UI

## Baseline `fts-engine` Profile

Default baseline settings for `fts-engine` in cross-engine runs:

- index: `slicedradix`
- language preset: `ftspreset.English()`
- scorer: `fts.WithScorer(fts.BM25())`
- filter: none
- persistence: `persist=none`
- field model: single-field body text

This baseline exists to keep the default cross-engine comparison simple and reproducible.

## Engine Matrix

Baseline engine set for MVP:

- `fts-engine`
- `bleve`
- `bluge`

Deferred engines:

- `tantivy`: allowed later through an explicit HTTP shim and documented as out-of-process
- `riot`: deferred until a fair English-oriented configuration is defined

## Datasets

The suite will support two dataset classes.

### Synthetic

Purpose:

- deterministic control runs
- throughput and latency comparison without external corpus dependencies
- stress-testing different corpus sizes and query concurrency levels

Baseline shape:

- Zipf-distributed vocabulary
- configurable document count
- configurable query count
- deterministic seed

### MS MARCO

Purpose:

- shared quality benchmark across engines
- realistic top-k ranking comparisons

Sampling rule for capped runs:

- always keep qrel-positive documents for kept queries
- fill the remaining document budget with deterministic reservoir sampling

This avoids the false-quality collapse that happens when a naive head slice drops relevant documents from the corpus.

## Shared Metrics

Every engine should report the same core fields where applicable:

- build duration
- documents per second
- search latency p50
- search latency p95
- search latency p99
- mean search latency
- QPS
- index size on disk
- Recall@k
- MRR
- nDCG@k

## `fts-engine`-Specific Extras

When diagnostics are enabled for `fts-engine`, the report may additionally include:

- execution strategy
- strategy skip reason
- postings read
- index lookups
- diagnostics timing breakdown
- WAND usage and skip reasons

These fields are extra metadata. They are not part of the cross-engine required minimum schema.

## Methodology Rules

The suite should follow these rules by default.

### Fairness

- Use one baseline analyzer family per engine and document differences explicitly.
- Do not apply engine-specific tuning in baseline runs.
- Keep query concurrency explicit and identical across engines for a given scenario.
- Keep top-k identical across engines for a given scenario.

### Query Semantics

The current `fts-engine` parser treats adjacent plain terms as a `Should`-style boolean query. Adapters for other engines should aim for the closest available bag-of-words semantics rather than silently switching to phrase matching.

If exact semantic parity is not possible, the difference must be documented in the adapter and README.

### Persistence

Default baseline runs for `fts-engine` use `persist=none`.

Implications:

- build and search metrics are still valid
- on-disk size for the default `fts-engine` profile will be `0`
- later iterations will add `persist=snapshot` and `persist=segment` scenarios so that size comparisons become meaningful for this engine

### Reproducibility

- deterministic shuffle seed
- deterministic dataset sampling seed
- explicit batch size
- explicit concurrency
- explicit warmup behavior

## Output Requirements

The suite must produce:

- machine-readable JSON output
- plain-text table output for local inspection

The JSON should contain at least:

- run metadata
- engine name
- engine configuration
- dataset configuration
- index metrics
- latency metrics
- quality metrics when qrels are available
- optional engine-specific extras

## Stable Report Schema

The JSON output format is versioned.

Current version:

- `schema_version = "benchmarks.v1alpha1"`

JSON layout:

- top-level object
- `schema_version`
- `records`

Each record contains:

- `engine`
- `run`
- `dataset`
- `config`
- `index`
- `latency`
- optional `quality`
- optional `extras`

`run` contains execution-environment and run-control metadata:

- `timestamp`
- `go_version`
- `goos`
- `goarch`
- `num_cpu`
- `concurrency`
- `batch_size`
- `warmup_frac`

`dataset` contains corpus metadata separate from runtime metadata:

- `name`
- `num_docs`
- `num_queries`
- optional `params`

Rules:

- additive fields are allowed in future schema revisions
- breaking field moves or renames require a new `schema_version`
- `extras` is reserved for engine-specific diagnostics and must not be required for cross-engine comparisons

## Existing Tooling Boundary

This benchmark suite complements, but does not replace, existing repository tooling.

- `cmd/bench` remains the native benchmark and evaluation command for this repository
- `internal/bench` remains the source of engine-native quality and diagnostics logic
- `benchmarks/` is the cross-engine comparison layer

## MVP Exit Criteria

Iteration 0 is complete when the project has an explicit written baseline that answers:

- which engines are in scope for MVP
- which engines are deferred
- which `fts-engine` configuration is the default baseline
- which datasets are required
- which metrics are mandatory
- which fields are `fts-engine`-specific extras
- which methodology constraints are considered part of the benchmark contract

## Next Iterations

Implementation after this spec proceeds in the following order:

1. module skeleton and harness interfaces
2. synthetic dataset and shared metrics
3. current-API `fts-engine` adapter
4. stable report schema
5. `bleve` and `bluge` adapters
6. MS MARCO loader and quality runs
