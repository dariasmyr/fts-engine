# demo

Interactive terminal demo app for `fts-engine`.

This module is intended to run inside the repository root workspace defined by `../go.work`. Its `go.mod` also keeps a local `replace ../` fallback so module-local commands keep resolving the root library module.

## Run

From repository root:

```bash
(cd demo && go run ./cmd/fts)
```

With an explicit config file:

```bash
(cd demo && go run ./cmd/fts -config ./config/config_local.yaml)
```

## Config

- default config path: `./config/config_local.yaml`
- example config: `./config/config_local_example.yaml`

The app depends on the public library module at `github.com/dariasmyr/fts-engine/v2/pkg/...` and keeps its app-specific code inside `demo/internal/*`.

Relevant FTS choices:

- `fts.index`: `slicedradix`, `hamt`, or `flat`
- `fts.pipeline.preset`: `custom` or `observability`
- custom pipeline filter settings apply only when `preset: custom`

For an observability-oriented run, select:

```yaml
fts:
  index: "flat"
  pipeline:
    preset: "observability"
```

Keep the default `custom` preset for the bundled Wikipedia corpus. When changing
the index or pipeline, use a fresh persistence path or rebuild persisted data.
Segments and mutable snapshots validate one pipeline identity per indexed field.

The demo uses `demo-custom` as a diagnostic name and derives compatibility from
the tokenizer and selected filter configuration. Rebuild persistence created with
legacy name/version-only fingerprints; those fingerprints cannot prove
historical pipeline compatibility and are rejected.

## Semantic search

The demo can optionally index the complete corpus selected by `dump_path` into
the semantic HNSW index. It uses Ollama with `nomic-embed-text` by default.

For a smaller build, set the root-level document limit. The loader stops reading
the compressed XML after reaching the limit, and both lexical and semantic
indexes receive the same corpus:

```yaml
dump_path: "./data/enwiki-latest-abstract1.xml.gz"
max_documents: 25000 # 0 indexes the complete dump
```

Use separate lexical and semantic persistence paths for limited builds. Loading
or overwriting a full-corpus generation with a limited corpus would produce a
measurement against the wrong persisted state.

```bash
ollama pull nomic-embed-text
ollama serve
```

Enable the `semantic` section from `config/config_local_example.yaml`:

```yaml
semantic:
  enabled: true
  workers: 2
  checkpoint_documents: 25000
  max_live_vectors: 1000000
  search:
    ef_search: 20
    candidate_chunks: 20
  ollama:
    base_url: "http://localhost:11434"
    model: "nomic-embed-text:latest"
    dimensions: 768
    context_tokens: 2048
    document_prefix: "search_document: "
    query_prefix: "search_query: "
  persistence:
    enabled: true
    path: "./data/semantic/local"
    load_on_start: true
    save_on_build: true
    max_file_bytes: 1073741824 # 1 GiB
    max_open_bytes: 8589934592 # 8 GiB
```

`candidate_chunks` is the per-request candidate budget (the service ceiling is
200). HNSW requires `max(configured_ef_search, candidate_chunks)` before the
per-segment index-size cap, so keep both values within the operational limit.

In the CUI, `Ctrl+T` cycles through `syntax`, `plain`, and `semantic` modes.
Semantic mode uses natural-language queries and displays cosine distance; lower
distance is better. Each wiki document is embedded as one combined
title/abstract/extract input, which keeps the current 244,014-document shard at
roughly one vector per document instead of one vector per source field.

The persisted semantic generation is model-bound. Changing the Ollama model,
dimensions, prefixes, or chunking requires a fresh persistence path or rebuild.
During a new build, the demo commits and persists a resumable checkpoint every
`semantic.checkpoint_documents` source documents. On restart it reads committed
document IDs from the saved generation and embeds only missing corpus documents.
With the default 25,000-document interval, an interruption loses at most the
currently uncommitted interval rather than the complete embedding run.
`semantic.persistence.max_open_bytes` is a validation budget, not a reserved
allocation. The persistence reader applies conservative multipliers to vector,
graph, and state files; the demo uses 8 GiB so the approximately 244k-vector
Wikipedia generation passes validation. Old generations and unreachable
segment objects are pruned after a successful checkpoint, retaining two
generations for recovery.

`semantic.persistence.max_file_bytes` bounds each persistence file. It is 1 GiB
in the demo because one compacted 244k × 768 `vectors.bin` file is about 715 MiB;
the library's lower default is sufficient only while those vectors remain split
across multiple segments.

### Semantic compaction

Compaction is an optional final step of normal semantic indexing. It runs in
place after missing documents are indexed and committed:

```yaml
semantic:
  compaction:
    enabled: true
    prune: true
    profile_path: "./data/profiles/semantic-compaction"
```

Run the demo normally. It loads or builds the index at
`semantic.persistence.path`, adds missing documents, compacts the resulting
segments, and saves the compacted generation. When `prune` is enabled, it first
reloads that generation and verifies its statistics, then prunes old generations
and unreachable segment objects before starting the CUI:

```bash
(cd demo && go run ./cmd/fts -config ./config/config_local.yaml)
```

If the current index already has at most one segment and no stale vectors,
compaction is skipped. This makes subsequent starts idempotent: an already
persisted compacted generation is loaded and is not rebuilt. If `prune` is
enabled on that later start, the generation is reloaded and verified before the
older backups are removed.

The `Semantic compaction summary` log contains compaction, save, and prune
durations; interval allocation and GC counters; heap snapshots before, after,
and after GC; segment and vector counts; and persistence sizes. `cpu.pprof` covers the
compaction interval. `heap.pprof` is captured after a GC, while `allocs.pprof`
contains cumulative process allocation samples. Inspect them with, for example,
`go tool pprof ./data/profiles/semantic-compaction/cpu.pprof`.

## Measure full-dump indexing

Set `mode.type: experiment`, enable semantic search, and use a fresh semantic
persistence path (or remove the old path) before running:

```bash
(cd demo && go run ./cmd/fts -config ./config/config_local.yaml)
```

The structured log reports separate measurements for:

- embedding (`documents_per_second`);
- HNSW construction (`Semantic HNSW committed`);
- persistence duration and on-disk bytes;
- each resumable semantic checkpoint and generation;
- total duration, current heap, and total allocated bytes.

The measurement covers every document in the single file named by `dump_path`.
The bundled `enwiki-latest-abstract1.xml.gz` currently contains 244,014
documents; it is one abstract-dump shard, not all English Wikipedia shards.
At 768 dimensions, one vector per bundled document has a raw float32 payload of
about 715 MiB; the HNSW graph, references, persistence metadata, and transient
build copies require additional disk and memory.
Run on an otherwise idle machine and keep the model, worker count, dump, and
Ollama settings unchanged when comparing results.

On Apple Silicon, start with two workers. Four workers favor maximum throughput
and can keep both CPU preprocessing and Metal inference continuously saturated,
which noticeably increases package temperature. Use one worker for the coolest
run, or four only when sustained maximum load is acceptable.
