# Fast Turtle Search Engine

Reusable full-text search library for Go.

It provides:

- mutable in-memory search via `pkg/fts`
- built-in indexes in `pkg/index/slicedradix`, `pkg/index/hamt`, and `pkg/index/flat`
- query-string, phrase, boolean, field-scoped, and prefix search
- optional pipelines, stemming, and language presets via `pkg/textproc` and `pkg/ftspreset`
- mutable snapshots and sealed read-only segments via `pkg/ftspersist`
- chunk-aware dense-vector search via `pkg/vector` and `pkg/semantic`
- immutable semantic generations via `pkg/semanticpersist`
- per-request diagnostics and aggregated search stats via `pkg/ftsstats`

## Public API Surface

For external integrations, prefer these public packages:

- `pkg/fts` - core engine, document model, query API
- `pkg/index/slicedradix` - exact, positional, and prefix index
- `pkg/index/hamt` - exact and positional index
- `pkg/index/flat` - pointer-light exact, positional, and prefix index for high-cardinality data
- `fts.WordKeys` - default token-to-key generator
- `pkg/ftspersist` - recommended snapshot and segment persistence API
- `pkg/posting` - storage-neutral ordinals, iterators, traversal, and export contracts
- `pkg/ftse` - low-level FTSE sealed segment reader and writer
- `pkg/presence` - optional ordinal-only FPR1 presence index
- `pkg/segmentbundle` - stream/blob bundle API
- `pkg/textproc` - tokenizers and filters
- `pkg/ftspreset` - ready-to-use pipeline presets
- `pkg/filter` - bloom, cuckoo, and ribbon filters
- `pkg/ftsstats` - aggregated search observability
- `pkg/vector` - dense-vector metrics, search contracts, and immutable result filters
- `pkg/vector/hnsw` - deterministic HNSW construction and immutable approximate search
- `pkg/embedding` - provider-neutral embedding clients and transformations
- `pkg/embedding/ollama` - Ollama `/api/embed` transport
- `pkg/semantic` - model-bound document indexing and chunk-aware semantic search
- `pkg/semanticencode` - document chunking and embedding adaptation
- `pkg/semanticpersist` - generation-based persistence for committed semantic index states

`cmd/*`, `internal/*`, and `benchmarks/*` are repository-owned tooling, not the main library surface.

## Requirements

- Go `1.25+`

## Install

```bash
go get github.com/dariasmyr/fts-engine/v2@latest
```

See [`MIGRATION.md`](MIGRATION.md) when upgrading from v1.

## Quickstart

```go
package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/index/slicedradix"
)

func main() {
	engine := fts.New(slicedradix.New(), fts.WordKeys)

	_ = engine.Index(context.Background(), fts.Document{ID: "doc-1", Fields: map[string]fts.Field{fts.DefaultField: {Value: "Wikipedia: Rosa is a French hotel barge"}}})
	_ = engine.Index(context.Background(), fts.Document{ID: "doc-2", Fields: map[string]fts.Field{fts.DefaultField: {Value: "Rosa runs hotel operations in France"}}})

	res, err := engine.SearchText(context.Background(), "french hotel", fts.SearchOptions{
		MaxResults: 10,
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("results=%d\n", res.TotalResultsCount)
	for _, item := range res.Results {
		fmt.Printf("id=%s unique=%d total=%d\n", item.ID, item.UniqueMatches, item.TotalMatches)
	}
}
```

Notes:

- `fts.New(...)` creates a single-field service backed by `fts.DefaultField`
- in practice that means the regular single-field index uses the field name `_default`
- if you do not set a pipeline, the default behavior is alphanumeric tokenization plus lowercasing
- add `fts.WithScorer(fts.BM25())` or `fts.WithScorer(fts.TFIDF())` when you want score-based ranking

## Semantic Search

`pkg/semantic` provides chunk-aware document indexing and direct text search.

Embedding providers implement the provider-neutral `embedding.Client` interface.
Every client is permanently bound to one validated `embedding.Model`; custom
functions can be adapted with `embedding.NewClient`. `semantic.New` creates the
document and query paths and applies model prefixes. `ServiceConfig` owns this
pipeline while its nested `IndexConfig` remains independent of the model:

```go
config, err := semantic.DefaultServiceConfig(client.Model())
if err != nil {
	return err
}
service, err := semantic.New(client, config)
if err != nil {
	return err
}
```

Service and index parameters are configured through `ServiceConfig`. Document
ingestion and query encoding have independent chunk
ceilings. `EncodingLimits` separately bounds source and embedding memory for the
built-in model pipeline:

```go
config, err := semantic.DefaultServiceConfig(client.Model())
if err != nil {
	return err
}
config.Index.Limits.MaxLiveVectors = 2_000_000
config.Index.Limits.MaxStaleVectors = 2_000_000
config.Index.Limits.MaxChunksPerDocument = 96
config.Index.Search = semantic.SearchConfig{
	MaxResults:         50,
	MaxCandidateChunks: 500,
	MaxChunksPerResult: 3,
	MaxQueryChunks:     96,
	MaxEfSearch:        500,
	MaxVisitLimit:      2_000_000,
}
config.Encoding.MaxFields = 16
config.Encoding.MaxSourceBytes = 1 << 20
config.Encoding.MaxEmbeddingBytes = uint64(96 * client.Model().Dimensions * 4)
service, err := semantic.New(client, config)
```

`semantic.New` owns a reproducible built-in encoding pipeline. Advanced callers
can create an index directly from `IndexConfig`, provide their own document and
query encoders, and bind them through `semantic.NewService`.

Index documents, commit pending vectors, and search them:

```go
ctx := context.Background()

if err := service.AddDocument(ctx, fts.Document{
	ID: "doc-1", Fields: map[string]fts.FieldData{"body": {Text: "French hotel on a river"}},
}); err != nil {
	return err
}
if err := service.AddDocument(ctx, fts.Document{
	ID: "doc-2", Fields: map[string]fts.FieldData{"body": {Text: "Boat operations in France"}},
}); err != nil {
	return err
}

// Commit makes all staged document mutations searchable.
if err := service.Commit(ctx); err != nil {
	return err
}

result, err := service.SearchWithConfig(ctx, "French hotel", 10, semantic.QueryConfig{
	EfSearch:        64,
	VisitLimit:      10_000,
	CandidateChunks: 200,
})
if err != nil {
	return err
}
for _, hit := range result.Hits {
	fmt.Printf("doc=%s distance=%.4f\n", hit.DocID, hit.Distance)
}
```

Text queries use the same splitter as indexed documents. Short queries produce
one embedding; long queries are split into overlapping chunks automatically.

`AddDocument`, `ReplaceDocument`, and `DeleteDocument` queue mutations. They
become visible to the committed read view after `Commit`. Replacement and
deletion keep old physical rows as stale rows; search excludes them through
liveness filters. Call `service.Compact(ctx)` to rebuild the in-memory HNSW
segments using only live rows and reclaim stale capacity.

The runtime is HNSW-first. It does not run an exact fallback when recall is
low. Increase `EfSearch`, `VisitLimit`, or `CandidateChunks` for a more
expensive search, and compact when stale-row accumulation becomes significant.

See the runnable [`semantic-commit` example](examples/client-library/semantic-commit/main.go).
The [`semantic-custom-encoder` example](examples/client-library/semantic-custom-encoder/main.go)
shows the low-level `NewIndex` + `NewService` path with caller-owned encoders.
The [`semantic-index` example](examples/client-library/semantic-index/main.go)
uses `semantic.Index` directly with caller-produced chunk and query embeddings.
The runnable
[`semantic-ollama` example](examples/client-library/semantic-ollama/main.go)
derives separate document/query input paths from the prefixes in
`client.Model()` while keeping one persisted semantic config. Advanced callers
can construct an index and individual encoders explicitly, then bind them with
`semantic.NewService`.

The low-level `semantic.Index` accepts raw finite vectors in
`semantic.ChunkEmbedding`. It copies and performs metric-specific preparation
exactly once on both indexing and query paths; callers do not need to normalize
cosine vectors themselves.

At the vector layer, `vector.Space.Prepare` returns an opaque
`vector.PreparedVector`, not a `[]float32`. Its backing storage cannot be
mutated by callers; `CopyTo` is the explicit bridge into trusted prepared
storage. Raw `[]float32` buffers remain only in the low-level
`PreparedVectorStore` contract so search can reuse caller-provided buffers
without allocating per distance calculation.

## Choosing an Index

| Index | Capabilities | When to use |
| --- | --- | --- |
| `slicedradix` | exact, positional, prefix | best default if you need prefix queries |
| `hamt` | exact, positional, prefix via term scan | use when you do not need prefix-heavy queries |
| `flat` | exact, positional, prefix via a lazy sorted term view | high-cardinality data where most terms occur in few documents, especially when building sealed segments |

Prefix queries require an index that implements `fts.PrefixIndex`. All built-in
mutable indexes support prefix queries, but with different performance profiles:
`slicedradix` performs structural prefix lookup, `hamt` scans stored terms, and
`flat` scans a lazily built sorted term view. Prefer `slicedradix` for
prefix-heavy workloads.

`flat` stores term bytes in a shared arena and keeps the first posting inline.
This reduces pointer overhead for log and telemetry datasets with many mostly
unique identifiers. Its first prefix search builds a sorted term view; exact
search does not require that initialization.

## Pipelines and Presets

Use a preset when the defaults fit your language mix:

```go
engine := fts.New(slicedradix.New(), fts.WordKeys, ftspreset.Multilingual())
```

Available presets:

- `ftspreset.English()`
- `ftspreset.Russian()`
- `ftspreset.Multilingual()`

Use `pkg/textproc` when you want an explicit pipeline:

```go
pipe, err := textproc.NewNamedPipeline(
	"custom-search",
	textproc.AlnumTokenizer{},
	textproc.LowercaseFilter{},
	textproc.MinLengthOrNumericFilter{MinLength: 2},
	textproc.EnglishStopwordFilter{},
	textproc.EnglishStemFilter{},
)
if err != nil {
	return err
}

engine := fts.New(slicedradix.New(), fts.WordKeys, fts.WithPipeline(pipe))
```

Configure field-specific pipelines with `fts.WithFieldPipelines`; other fields use
the service-level pipeline configured through `fts.WithPipeline`. Indexing and
query execution use the same configured pipeline for each field.

Use a named pipeline when its output will be persisted in a sealed segment:

```go
pipe, err := textproc.NewNamedPipeline(
	"support-events",
	textproc.AlnumTokenizer{},
	textproc.LowercaseFilter{},
)
if err != nil {
	return err
}
```

`textproc` fingerprints the tokenizer fingerprint and ordered filter fingerprints.
The pipeline name is diagnostic metadata and does not affect compatibility.
Built-in components include their effective settings, Unicode table version, and
the pinned Snowball implementation or dictionary identity. Changing a tokenizer,
filter order, component setting, or implementation identity changes the pipeline
fingerprint automatically.

Custom tokenizers and filters implement `Fingerprint() string`. Return a stable,
self-describing value such as `sha256:<64 lowercase hexadecimal characters>` that
covers every input affecting token generation. Include a unique component domain,
an internal configuration version, and all effective settings in the hashed data.
Use `fts.SHA256Fingerprint` after constructing deterministic configuration bytes.
No reflection or implementation hashing is used.

External implementations of `fts.Pipeline` construct their own
`fts.PipelineDescriptor` with a self-describing fingerprint. The engine validates
the fingerprint format and compares it exactly, but cannot prove that a custom
implementation honestly fingerprints its behavior.

Current fingerprints use the form `sha256:<digest>`. For `textproc`, the hashed
canonical stream contains the versioned pipeline domain, tokenizer fingerprint,
filter count, and filter fingerprints in execution order. Strings and counts are
length-prefixed so different component sequences cannot produce the same canonical
bytes through ambiguous concatenation.

### Observability Pipeline

`textproc.ObservabilityPipeline()` is intended for logs and other technical
text. It keeps complete technical atoms together with searchable components:

```text
10.0.0.1        -> 10.0.0.1, 10, 0, 1
io.EOF          -> io.eof, io, eof
checkout-api/v2 -> checkout-api/v2, checkout, api, v2
```

```go
engine := fts.New(
	flat.New(),
	fts.WordKeys,
	fts.WithPipeline(textproc.ObservabilityPipeline()),
)
```

The preset lowercases tokens, keeps numeric and two-character diagnostic terms,
and deliberately applies no stemming or stop-word removal. It also removes
duplicate tokens from each processed value. That makes it presence-oriented:
repeated terms do not increase term frequency, and repeated-token phrase
semantics are not preserved.

## Search API

The v2 API has two search entry points:

- `SearchText(...)` parses query-string syntax;
- `SearchQuery(...)` executes typed query AST values.

Both accept `SearchOptions` for result limits, candidate IDs, a default field,
and an optional field subset. For bag-of-words input without query syntax, pass
`TermQuery` to `SearchQuery`.

Supported query-string syntax:

- `hotel`
- `french hotel`
- `"hotel barge"`
- `+hotel -market`
- `title:hotel`
- `title:"hotel barge"`
- `bar*`
- `+(title:hotel title:french) -market`

Programmatic queries are available through the AST types in `pkg/fts` such as `TermQuery`, `PhraseQuery`, `NearPhraseQuery`, `PrefixQuery`, and `BooleanQuery`.

Candidate-aware searches use the unified options API:

```go
result, err := engine.SearchText(ctx, "postgres checkpoint", fts.SearchOptions{
	CandidateIDs: []fts.DocID{"event-10", "event-42"},
	MaxResults:   100,
})
```

Use `SearchQuery` with the same options for a typed query AST. A nil
`CandidateIDs` slice is unrestricted; a non-nil empty slice matches nothing.

Clients that already own an ordinal mapping and only need streamed exact-term
presence checks can bypass `Service` and `DocRegistry`:

```go
candidateCursor := posting.NewBitmapIterator(candidateWords)
err := posting.IntersectWithIterator(
	ctx,
	segmentReader,
	analyzedKeys,
	candidateCursor,
	candidateCardinality,
	func(ord posting.Ord) bool {
		// Consume the matching ordinal. Return false to stop successfully.
		return true
	},
)
```

The bitmap word slice uses little-endian bit numbering within each `uint64`:
bit `n` represents `DocOrd(n)`. It must remain immutable until the iterator is
closed. The low-level API does not tokenize, score, hide tombstones, or map
external document IDs.

Field behavior summary:

- with `fts.New(...)`, documents are indexed only into `_default`
- with `fts.NewMultiField(...)`, the service keeps a separate index per field name
- field indexes in multi-field mode are created lazily on first indexing of that field
- searching a field that has no index does not return an error; it returns zero matches

What that means for searches:

- an unscoped query on a single-field service searches only `_default`;
- an unscoped query on a multi-field service searches all existing field indexes;
- an explicit AST field or query-string field such as `title:hotel` returns no matches when that field has never been indexed;
- `SearchOptions.Fields` restricts unqualified clauses to the provided existing fields; missing fields are ignored;
- different field-specific clauses can be combined with a typed `BooleanQuery`;
- prefix search behaves the same with one extra rule: if the field exists but its index does not support prefix search, that field contributes no prefix matches
- phrase search behaves the same: if the field exists but its index does not implement `fts.PositionalIndex`, that field contributes no phrase matches

## Multi-Field Services

Use `fts.NewMultiField(...)` when documents have separate searchable fields:

```go
factory := func(string) (fts.Index, error) {
	return slicedradix.New(), nil
}

engine := fts.NewMultiField(factory, fts.WordKeys)

_ = engine.Index(context.Background(), fts.Document{
	ID: "doc-1",
	Fields: map[string]fts.Field{
		"title":    {Value: "French hotel"},
		"abstract": {Value: "Rosa runs hotel operations in France"},
	},
})

res, _ := engine.SearchQuery(
	context.Background(),
	fts.TermQuery{Field: "title", Term: "hotel"},
	fts.SearchOptions{MaxResults: 10},
)
fmt.Println(res.TotalResultsCount)
```

In this mode, you usually create one index per field through the factory. The engine calls the factory the first time a field needs to be indexed and then reuses that index for future documents in the same field.

## Rank Profiles

Use `fts.RankProfile` when fields or query types should contribute differently to
ranking. A rank profile applies multipliers on top of an existing scorer such as
BM25 or TF-IDF:

```go
engine := fts.NewMultiField(
	factory,
	fts.WordKeys,
	fts.WithRankProfile(fts.RankProfile{
		Name: "docs",
		Base: fts.BM25(),
		FieldWeights: fts.FieldWeights{
			"title":    3.0,
			"abstract": 1.0,
		},
		QueryTypeWeights: fts.QueryTypeWeights{
			Term:   1.0,
			Prefix: 0.6,
			Phrase: 4.0,
		},
	}),
)
```

The weights are relative ranking multipliers. They are not universal defaults and
they are not score thresholds. For example, `"title": 3.0` means each title match
contributes three times its base BM25/TF-IDF score. It does not mean the final
document score is capped at `3.0`.

Query type weights are also relative multipliers. They are useful for mixed
queries, where exact terms, phrases, and prefixes can compete in the same result
set.

For lower-level composition, `fts.WeightedScorer` can also be passed directly to
`fts.WithScorer(...)`.

The lower-level scorer API is still available when you want to choose a scorer
implementation directly:

```go
engine := fts.NewMultiField(
	factory,
	fts.WordKeys,
	fts.WithScorer(fts.BM25()),
)
```

## HNSW Vector Index

`pkg/vector/hnsw` provides observable deterministic construction, an immutable
packed HNSW index, and a binary graph format bound to a separate authoritative
vector file. Production search is HNSW-only; the independent exhaustive exact
search lives in the benchmark module.

`pkg/semantic` uses immutable HNSW segments and an explicit flush lifecycle.
Ordinary semantic search is in-memory and does not require disk persistence.

```go
config := hnsw.Config{
	Build: hnsw.BuildConfig{
		MaxNeighbors: 8, EfConstruction: 64, Seed: 1,
	},
	Search: hnsw.SearchConfig{
		EfSearch: 32, VisitLimit: 10_000,
	},
}

index, err := hnsw.Build(ctx, preparedVectors, config)
if err != nil {
	return err
}
result, err := index.Search(ctx, query, 10, hnsw.QueryConfig{EfSearch: 64})
```

`hnsw.Build` reads prepared rows in stable dense ordinal order and hides manual
graph construction. Use `index.Report()` to inspect build provenance, search
limits, graph structure, and packed storage. Run parameter sweeps with
`go run ./cmd/vector-search` from `benchmarks/`.

## Score Explanation

Use `Explain(...)` to inspect why a specific document received its score for a
query:

```go
explanation, err := engine.Explain(context.Background(), "postgres backup", "doc-1")
if err != nil {
	panic(err)
}

fmt.Printf("id=%s matched=%t score=%.4f\n", explanation.ID, explanation.Matched, explanation.Score)
for _, c := range explanation.Contributions {
	fmt.Printf(
		"field=%s term=%s query_type=%s base=%.4f field_weight=%.2f query_type_weight=%.2f score=%.4f\n",
		c.Field,
		c.Term,
		c.QueryType,
		c.BaseScore,
		c.FieldWeight,
		c.QueryTypeWeight,
		c.Score,
	)
}
```

For weighted scoring, each contribution includes both the base BM25/TF-IDF score
and the applied field and query type weights.

## Persistence

Lexical and semantic search use separate persistence APIs. Use `pkg/ftspersist`
for `pkg/fts` snapshots and sealed segments, and `pkg/semanticpersist` for
immutable semantic generations.

### Lexical Persistence

| Mode | Writable after load | Recommended API | Notes |
| --- | --- | --- | --- |
| snapshot | yes | `SaveSnapshot`, `LoadSnapshot` | restores a mutable service |
| segment | no | `SaveSegment`, `LoadSegment` | restores a sealed read-only service |

Important details:

- snapshot and segment formats are different and not interchangeable
- `mmap` is available only for segments via `ftspersist.SegmentLoadOptions{Access: ftspersist.AccessMmap}`
- `pkg/ftse` is a low-level API for raw FTSE segments; prefer `pkg/ftspersist` for filesystem persistence
- do not mutate a service while `SaveSnapshot(...)` is running; searches may continue concurrently
- do not mutate a service while `SaveSegment(ctx, ...)` is running; searches may continue concurrently, and the completed immutable generation is published atomically
- snapshots are written as one atomic bundle containing the index, state, and optional filter; legacy split index/filter snapshots remain readable
- segment persistence retains the current and immediately previous immutable generations; loaded generations are protected by leases, and unused older generations are pruned before the next generation is published
- new segment writes use FTSE v3; FTSE v2 is read-only compatibility, and FTSE v1 must be rebuilt from source data
- FTSE v3 validates lookup metadata on open and data blocks lazily; use `ftse.Reader.VerifyAll(ctx)` for eager full verification
- `pkg/segmentbundle` stores a complete sealed index as one `io.Reader`/`io.Writer` bundle
- create one `fts.SnapshotRegistry`, register built-in codecs with `ftsbuiltin.RegisterSnapshotCodecs(registry)`, and pass it through persistence options

Snapshots, filesystem segment manifests, and segment bundles store one pipeline
descriptor per indexed field. Supply matching default and field pipelines when
restoring the service:

```go
pipe := textproc.ObservabilityPipeline()
loaded, err := ftspersist.LoadSegment(
	ftspersist.SegmentPaths{Dir: "./data/segments/events"},
	fts.WordKeys,
	ftspersist.SegmentLoadOptions{
		Access: ftspersist.AccessFile,
	},
	fts.WithPipeline(pipe),
)
```

Restore validates pipeline fingerprints against the configured pipelines before
returning the service. Current snapshots and segment manifests require pipeline
and key-generator metadata; versions containing legacy analyzer metadata must be
rebuilt. Legacy bare `SHA256(name + "@" + version)` fingerprints are never
accepted or migrated because they did not record pipeline configuration.
Callers can detect this condition with
`errors.Is(err, fts.ErrPersistenceRebuildRequired)`; the demo falls back to a
fresh service so its normal indexing pass rebuilds persistence from source data.
Pipeline-bearing single snapshots use format version 5, multi-field snapshots
use version 6, filesystem segment manifests use version 5, and bundles use
version 6. Filter snapshot versions are unchanged. New raw FTSE segment writes
use v3; readers retain v2 compatibility, while v1 indexes must be rebuilt.

`AccessFile` keeps the term table resident and reads matching posting blocks from
the file. `AccessMmap` maps the segment. FTSE v3 open validates lookup metadata;
block checksums are validated before their data is returned. Callers must close
the returned loaded service.

Current working persistence examples:

- `examples/client-library/snapshot-save-files/main.go`
- `examples/client-library/snapshot-load-files/main.go`
- `examples/client-library/snapshot-load-files-low-level/main.go`
- `examples/client-library/segment-save-files/main.go`
- `examples/client-library/segment-load-files/main.go`
- `examples/client-library/segment-load-mmap/main.go`
- `examples/client-library/segment-bundle/main.go`

See `examples/client-library/README.md` for the exact run order. The load examples expect artifacts created by the corresponding save examples.

### Semantic Persistence

Create the model-bound service in memory, then atomically commit and capture the
revision that will be saved:

```go
config, err := semantic.DefaultServiceConfig(embeddingClient.Model())
if err != nil {
	return err
}
service, err := semantic.New(embeddingClient, config)
if err != nil {
	return err
}

if err := service.AddDocument(ctx, document); err != nil {
	return err
}
generation, err := semanticpersist.CommitAndSaveService(
	ctx,
	"./data/semantic",
	service,
	semanticpersist.SaveConfig{
		ExpectedGeneration: &semanticpersist.Generation{},
	},
)
```

`CommitAndSaveService` prepares one revision, persists that exact candidate, and
only then makes it searchable. The generation stores `IndexConfig` plus the
service-level model, chunking, prefixes, and encoding limits. `LoadService`
restores that configuration and only requires a matching embedding client:

```go
loaded, generation, err := semanticpersist.LoadService(
	ctx,
	"./data/semantic",
	embeddingClient,
	semanticpersist.LoadConfig{},
)
service = loaded
```

Raw indexes and external encoders use `Save`/`CommitAndSave` and `Load` instead.
Reconstruct external encoders and bind them through `semantic.NewService`.

See the runnable
[`semantic-persistence` example](examples/client-library/semantic-persistence/main.go)
for the model-backed save and load flow. The
[`semantic-index-persistence` example](examples/client-library/semantic-index-persistence/main.go)
shows raw `Index` persistence with caller-owned embeddings.

Replacement and deletion leave stale physical vectors in the service until
`service.Compact(ctx)` is called. Compaction preserves stable `VectorID` values
and removes stale rows from the new committed in-memory view. Persistence does
not delete old on-disk generations; retention and garbage collection are
separate operations. `Compact` publishes the rewritten layout in memory without
persistence I/O. Call `semanticpersist.Save` or `semanticpersist.SaveService`
afterward to persist that layout. A crash before that save restores the previous
layout; if the current logical revision was already durable, only the compaction
work is lost.

`CommitAndSave` holds the index mutation gate through preparation, persistence,
and publication. Concurrent mutations wait, while searches continue against the
previous committed view. A stale generation or failure before replacement of
`CURRENT` leaves the candidate invisible and its mutations pending for retry.
After successful persistence, the matching view is made searchable with a
non-failing in-memory transition. `ErrIndeterminate` means `CURRENT` may already
reference the candidate; the candidate is therefore made searchable and a
subsequent `Load` determines the current generation. A loaded semantic service
owns no filesystem resources and requires no persistence cleanup. Saving always
uses file and directory `fsync` before returning successfully.

Advanced callers that manage an already committed `semantic.Index` directly can
use the low-level `semanticpersist.Save` and `semanticpersist.Load` operations.
`Save` rejects pending mutations. Saving requires an explicit expected
generation. `CommitAndSave`, `Save`, `Load`, `Prune`, and `RecoverGeneration`
acquire a filesystem lock only for the duration of the operation; write
operations always fully sync their changes. Operation locking is supported on
Linux, macOS, and FreeBSD.

Intermediate single-segment semantic formats are rejected rather than migrated.
Semantic runtime uses HNSW-only immutable segments; exact comparison remains a
benchmark-owned `exact.Searcher` and is not a production fallback.

`RecoverGeneration` provides an explicit recovery flow: it fully validates a
selected generation and its referenced objects before replacing `CURRENT`.

### Stream Bundles

Use `pkg/segmentbundle` when the sealed index should be stored as one self-contained
blob instead of a filesystem directory. `SaveBundle` writes to any
`io.Writer`, and `LoadBundle` reads from any `io.Reader`, so the blob can be
stored in memory, a database, object storage, or a network stream:

```go
var blob bytes.Buffer
if err := segmentbundle.SaveService(&blob, service); err != nil {
	panic(err)
}

bundle, err := segmentbundle.LoadBundle(bytes.NewReader(blob.Bytes()))
if err != nil {
	panic(err)
}
restored, err := segmentbundle.RestoreService(bundle, fts.WordKeys)
```

This bundle format is separate from `ftspersist.SaveSegment`: it has no
filesystem manifest, atomic file replacement, or mmap access mode. New bundles
store a pipeline descriptor for every field and validate them during restore.
Use `fts.WithFieldPipelines` when restoring field-specific pipelines. See
`examples/client-library/segment-bundle` for a complete round-trip example.

The v6 bundle format requires configuration fingerprints; bundles created by older
formats must be rebuilt.

When exporting custom `posting.Exporter` values directly, pass pipeline identities
through `segmentbundle.BundleSaveOptions{Pipelines: ...}` instead of using
`SaveService`.

## Diagnostics and Stats

Per-request diagnostics are opt-in:

```go
ctx := fts.WithDiagnostics(context.Background())
res, _ := engine.SearchText(ctx, "postgres checkpoint", fts.SearchOptions{
	MaxResults: 10,
})

fmt.Println(res.Diagnostics.LogicalQueryType)
fmt.Println(res.Diagnostics.ExecutionStrategy)
fmt.Println(res.Diagnostics.Timings.Total)
```

For aggregated observability across many requests, use `pkg/ftsstats`:

```go
stats := ftsstats.NewSearchStats(64)
stats.ObserveResult("postgres checkpoint", res, nil)
snap := stats.Snapshot()
fmt.Println(len(snap.ByStrategy))
```

Runtime diagnostics are separate from `textproc.ObservabilityPipeline()`.

## Client Examples

`examples/client-library` contains the examples that match the current public API.

- `default` - minimal in-memory usage
- `preset` - preset pipeline via `pkg/ftspreset`
- `custom-options` - custom pipeline and filter
- `flat-observability` - flat index with technical-token analysis
- `segment-pipeline-compatibility` - pipeline-compatible sealed segment restore
- `rank-profile` - multi-field ranking with weighted field scoring
- `semantic-commit` - mutable document search with explicit commit visibility
- `semantic-custom-encoder` - low-level semantic index with caller-owned encoders
- `semantic-index` - direct semantic index usage with caller-produced vectors
- `semantic-persistence` - persisted writable restart and generation update
- `snapshot-*` - mutable snapshot save and restore
- `segment-*` - sealed segment save and restore, including `mmap`

All of these examples currently build and run from repository root.

## Repository Tooling

This repository also contains project-specific tooling:

- `demo/` - demo app module
- `benchmarks/` - benchmark suite and reports

If you need those flows, use their local docs instead of treating them as the main library entry point.

## Tests

Run public-package tests:

```bash
go test ./pkg/...
```

Run all tests:

```bash
go test ./...
```

The repository uses a root `go.work` workspace. Run multi-module commands from repository root or from a submodule directory inside this workspace. The child modules also keep a local `replace ../` fallback so current Go tooling can resolve the root library module consistently during module-local commands.

Run the demo module tests:

```bash
(cd demo && go test ./...)
```

Run the benchmarks module tests:

```bash
(cd benchmarks && go test ./...)
```

After Go build/test checks pass, run repository dependency policy checks:

```bash
go run ./tools/depcheck
```

`depcheck` is a post-toolchain architecture check. It validates only the repository's stable architecture boundaries:

- `pkg/*` may depend only on `pkg/*` inside this repository
- `examples/*` may import only public `pkg/*`
- `demo/*` may import only public `pkg/*` and `demo/internal/*`
- `benchmarks/*` may import only public `pkg/*`, `benchmarks/internal/*`, and `benchmarks/adapters/*`

`depcheck` does not try to validate every possible external dependency or historical path name. Its scope is the permanent internal module and package boundary policy.

It also does not duplicate `internal` import restrictions that the Go toolchain already enforces during `go build` and `go test`.

## Acknowledgements

Special thanks to:

- [@flrdv](https://github.com/flrdv)
- [@dmedovich](https://github.com/dmedovich) 

Thank you for helping make `fts-engine` better ❤️
