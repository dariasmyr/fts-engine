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
```

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
