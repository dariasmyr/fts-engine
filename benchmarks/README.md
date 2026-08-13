# benchmarks

Cross-engine benchmark suite for `fts-engine`, `bleve`, and `bluge`.

This module is the repository benchmark layer under `benchmarks/`.

It is intended to run inside the repository root workspace defined by `../go.work`. Its `go.mod` also keeps a local `replace ../` fallback so module-local commands keep resolving the root library module.

Current default baseline:

- `-concurrency=1`
- `fts-engine -persist=snapshot`

## What It Can Run

Datasets:

- `synthetic`: deterministic synthetic corpus
- `msmarco`: natural-language quality benchmark
- `wiki-typed`: real-data typed query benchmark

Engines:

- `fts-engine`
- `bleve`
- `bluge`
- `mock`: internal-only, useful only for harness bring-up

Output:

- plain-text summary table
- JSON report via `-out`

## Quick Start

Recommended synthetic baseline (heavier comparison run):

```bash
(cd benchmarks && go run ./cmd/bench \
  -engines=fts-engine,bleve,bluge \
  -dataset=synthetic \
  -synth-docs=100000 \
  -synth-queries=2000 \
  -concurrency=1 \
  -persist=snapshot \
  -out=./results/local/synthetic-baseline.json \
  -k=10)
```

MS MARCO run:

```bash
(cd benchmarks && go run ./cmd/bench \
  -engines=fts-engine,bleve,bluge \
  -dataset=msmarco \
  -msmarco-dir=./data/msmarco \
  -max-docs=100000 \
  -max-queries=2000 \
  -concurrency=1 \
  -persist=snapshot \
  -out=./results/local/msmarco.json \
  -k=10)
```

Wiki typed run:

```bash
(cd benchmarks && go run ./cmd/bench \
  -engines=fts-engine,bleve,bluge \
  -dataset=wiki-typed \
  -wiki-dump=./data/wiki/simplewiki-latest-pages-articles.xml.bz2 \
  -wiki-cache-dir=./data/wiki/wiki-typed-cache \
  -max-docs=50000 \
  -typed-queries=200 \
  -query-types=term,and-hh,and-hl,or-hh,phrase,prefix \
  -concurrency=1 \
  -persist=snapshot \
  -out=./results/local/wiki-typed.json \
  -k=10)
```

Write JSON output:

```bash
(cd benchmarks && go run ./cmd/bench \
  -engines=fts-engine,bleve,bluge \
  -dataset=synthetic \
  -out=./results/local/synth.json)
```

## Supported Modes

### `synthetic`

Use when you want:

- deterministic runs
- fast smoke tests
- throughput/latency comparison without external data

Required flags:

- `-dataset=synthetic`

Useful flags:

- `-synth-docs`
- `-synth-queries`
- `-words-per-doc`
- `-words-per-query`
- `-vocab-size`
- `-zipf-s`

### `msmarco`

Use when you want:

- realistic natural-language queries
- shared quality metrics across engines

Required flags:

- `-dataset=msmarco`
- `-msmarco-dir=/path/to/msmarco`

Useful flags:

- `-max-docs`
- `-max-queries`
- `-k`

Expected files inside `-msmarco-dir`:

- `collection.tsv`
- `queries.dev.small.tsv`
- `qrels.dev.small.tsv`

### `wiki-typed`

Use when you want:

- typed query classes on real data
- separate results for `term`, `and-hh`, `and-hl`, `or-hh`, `phrase`, `prefix`
- benchmark-side exact semantics with generated qrels

Required flags:

- `-dataset=wiki-typed`
- `-wiki-dump=/path/to/wiki.xml|wiki.xml.gz|wiki.xml.bz2`

Download the Simple English Wikipedia dump used by the examples:

```bash
mkdir -p benchmarks/data/wiki
curl -fL \
  https://dumps.wikimedia.org/simplewiki/latest/simplewiki-latest-pages-articles.xml.bz2 \
  -o benchmarks/data/wiki/simplewiki-latest-pages-articles.xml.bz2
```

`benchmarks/data/` is ignored by git, so fresh checkouts need to download the dump locally before running `wiki-typed`.

Useful flags:

- `-wiki-cache-dir`
- `-max-docs`
- `-typed-queries`
- `-query-types=term,and-hh,and-hl,or-hh,phrase,prefix`
- `-high-skip-top`
- `-high-pool`
- `-low-pool`
- `-prefix-min-expand`
- `-prefix-max-expand`

Important notes:

- parsed docs, generated queries, and generated qrels are cached
- cache files are split into:
  - `*.docs.json`
  - `*.queries.json`
  - `*.qrels.json`
  - `*.manifest.json`
- the benchmark builds each engine index once per run, then executes all query classes on that ready index
- `BUILD(s)`, `docs/s`, `INDEX(MB)`, `HEAP(MB)`, and `HEAP_OBJS` are therefore shared across query classes for the same engine in the same run

## Common Flags

Available for all dataset modes:

- `-engines=fts-engine,bleve,bluge`
- `-dataset=synthetic|msmarco|wiki-typed`
- `-k=10`
- `-out=./results/run.json`
- `-work=./work`
- `-batch=1000`
- `-warmup=0.10`
- `-concurrency=1`
- `-seed=0xC0FFEE`

What they mean:

- `-engines`: engines to compare
- `-dataset`: corpus mode
- `-k`: top-k used for quality metrics
- `-out`: optional JSON output path
- `-work`: engine work directory
- `-batch`: indexing batch size
- `-warmup`: fraction of queries used only for warmup
- `-concurrency`: parallel search workers
- `-seed`: deterministic shuffle and sampling seed

## `fts-engine` Flags

These matter only for `fts-engine` runs:

- `-index=slicedradix|hamt|flat`
- `-scorer=none|bm25|tfidf`
- `-lang=none|en|ru|multi|observability`
- `-filter=none|bloom|cuckoo|ribbon`
- `-persist=none|snapshot|segment`
- `-diagnostics`

Default `fts-engine` persistence in this suite is `snapshot`.

Despite its historical name, `-lang` selects the `fts-engine` analysis preset.
The `observability` value uses `textproc.ObservabilityPipeline()` and is intended
for technical identifiers. It affects only `fts-engine`; the Bleve and Bluge
adapters retain their own analyzers.

Examples:

Run `fts-engine` on `hamt`:

```bash
(cd benchmarks && go run ./cmd/bench \
  -engines=fts-engine \
  -dataset=msmarco \
  -msmarco-dir=./data/msmarco \
  -out=./results/local/fts-hamt-msmarco.json \
  -index=hamt)
```

Run the flat index with observability analysis:

```bash
(cd benchmarks && go run ./cmd/bench \
  -engines=fts-engine \
  -dataset=synthetic \
  -synth-docs=5000 \
  -synth-queries=500 \
  -index=flat \
  -lang=observability \
  -persist=segment)
```

The synthetic corpus is useful as a functional and performance smoke test, but
it is not a representative logs/traces corpus. Keep index, analysis preset, and
dataset independently configurable for controlled comparisons.

Measure persisted `snapshot` output:

```bash
(cd benchmarks && go run ./cmd/bench \
  -engines=fts-engine \
  -dataset=wiki-typed \
  -wiki-dump=./data/wiki/simplewiki-latest-pages-articles.xml.bz2 \
  -out=./results/local/fts-snapshot.json \
  -persist=snapshot)
```

Disable persistence for pure in-memory `fts-engine` runs:

```bash
(cd benchmarks && go run ./cmd/bench \
  -engines=fts-engine \
  -dataset=synthetic \
  -synth-docs=100000 \
  -synth-queries=2000 \
  -out=./results/local/fts-inmemory.json \
  -persist=none)
```

Enable diagnostics:

```bash
(cd benchmarks && go run ./cmd/bench \
  -engines=fts-engine \
  -dataset=wiki-typed \
  -wiki-dump=./data/wiki/simplewiki-latest-pages-articles.xml.bz2 \
  -out=./results/local/fts-diagnostics.json \
  -diagnostics)
```

## Runtime Flow

### `synthetic`

1. Generate synthetic docs and queries.
2. Build each engine index.
3. Force GC and measure retained heap before queries.
4. Run warmup queries.
5. Run measured queries.
6. Print table and optionally write JSON.

### `msmarco`

1. Load `collection.tsv`, queries, and qrels.
2. Apply caps from `-max-docs` and `-max-queries`.
3. Build each engine index.
4. Force GC and measure retained heap before queries.
5. Run warmup queries.
6. Run measured queries.
7. Print table and optionally write JSON.

### `wiki-typed`

1. Load docs from wiki dump or `*.docs.json` cache.
2. Load typed workload from `*.queries.json` + `*.qrels.json`, or regenerate it.
3. Refresh `*.manifest.json`.
4. Build each engine index once.
5. Force GC and measure retained heap before queries.
6. Run each query class separately on the same ready index.
7. Print grouped table and optionally write JSON.

Expected progress logs look like:

```text
bench: loading dataset=wiki-typed
wiki-typed: docs cache hit file=...
wiki-typed: queries cache hit file=...
wiki-typed: qrels cache hit file=...
wiki-typed: manifest ready file=...
bench: preparing engine=fts-engine
bench: engine=fts-engine index ready build_ms=... index_bytes=...
bench: running engine=fts-engine query_class=term docs=... queries=...
```

## Output Semantics

Main table columns:

- `BUILD(s)`: index build duration
- `docs/s`: indexing throughput
- `INDEX(MB)`: on-disk index size reported by the adapter
- `HEAP(MB)`: post-GC retained Go heap delta after index build and commit
- `HEAP_OBJS`: post-GC retained Go heap object-count delta
- `p50/p95/p99`: search latency percentiles
- `QPS`: measured search throughput
- `Recall@k`, `nDCG@k`, `MRR`: quality metrics

Heap values are measured against a post-GC baseline taken immediately before
the engine opens. They describe the ready index before warmup/search and are
retained-memory metrics, not build-time peaks. They exclude mmap, native memory,
RSS, and filesystem cache. Run index variants in separate processes for the
most reliable comparison.

For `wiki-typed`:

- quality metrics come from benchmark-side exact semantics over the corpus
- they are not human relevance labels like MS MARCO qrels

## Practical Defaults

If you do not pass overrides, this suite currently assumes:

- `-concurrency=1`
- `-warmup=0.10`
- `-batch=1000`
- `-k=10`
- `fts-engine -persist=snapshot`

Recommended starting points:

- smoke check: `synthetic`, `5000 docs`, `500 queries`
- realistic synthetic baseline: `100000 docs`, `2000 queries`
- MS MARCO baseline: `100000 docs`, `2000 queries`
- wiki-typed baseline: `50000 docs`, `200 queries per class`

## Automation

Fetch MS MARCO files:

```bash
./benchmarks/scripts/fetch-msmarco.sh ./benchmarks/data/msmarco
```

Run the built-in suite script:

```bash
MSMARCO_DIR=./data/msmarco ./benchmarks/scripts/run-suite.sh
```

`run-suite.sh` runs benchmark commands from the `benchmarks/` module, so
`MSMARCO_DIR=./data/msmarco` points at `./benchmarks/data/msmarco` when the script
is launched from the repository root.

Aggregate JSON shards:

```bash
(cd benchmarks && go run ./cmd/aggregate results/full)
```

## Notes From Current Package Review

Nothing obviously dead or broken stands out in the current `benchmarks/` code after the recent refactors.

Two things are still intentionally kept even though they are not core user-facing paths:

- `adapters/mock`: useful for harness bring-up and tests
- `cmd/aggregate` legacy JSON-array fallback: harmless compatibility layer for older shards

If you want, those can be removed later as a separate cleanup pass, but they are not urgent.
