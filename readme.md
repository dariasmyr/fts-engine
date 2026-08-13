# Fast Turtle Search Engine

Reusable full-text search library for Go.

It provides:

- mutable in-memory search via `pkg/fts`
- built-in indexes in `pkg/index/slicedradix`, `pkg/index/hamt`, and `pkg/index/flat`
- query-string, phrase, boolean, field-scoped, and prefix search
- optional pipelines, stemming, and language presets via `pkg/textproc` and `pkg/ftspreset`
- mutable snapshots and sealed read-only segments via `pkg/ftspersist`
- per-request diagnostics and aggregated search stats via `pkg/ftsstats`

## Public API Surface

For external integrations, prefer these public packages:

- `pkg/fts` - core engine, document model, query API
- `pkg/index/slicedradix` - exact, positional, and prefix index
- `pkg/index/hamt` - exact and positional index
- `pkg/index/flat` - pointer-light exact, positional, and prefix index for high-cardinality data
- `pkg/keygen` - token-to-key generators
- `pkg/ftspersist` - recommended snapshot and segment persistence API
- `pkg/segment` - lower-level sealed segment API
- `pkg/textproc` - tokenizers and filters
- `pkg/ftspreset` - ready-to-use pipeline presets
- `pkg/filter` - bloom, cuckoo, and ribbon filters
- `pkg/ftsstats` - aggregated search observability

`cmd/*`, `internal/*`, and `benchmarks/*` are repository-owned tooling, not the main library surface.

## Requirements

- Go `1.25+`

## Install

```bash
go get github.com/dariasmyr/fts-engine@latest
```

## Quickstart

```go
package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func main() {
	engine := fts.New(slicedradix.New(), keygen.Word)

	_ = engine.Index(context.Background(), fts.Document{ID: "doc-1", Fields: map[string]fts.Field{fts.DefaultField: {Value: "Wikipedia: Rosa is a French hotel barge"}}})
	_ = engine.Index(context.Background(), fts.Document{ID: "doc-2", Fields: map[string]fts.Field{fts.DefaultField: {Value: "Rosa runs hotel operations in France"}}})

	res, err := engine.SearchDocuments(context.Background(), "french hotel", 10)
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
engine := fts.New(slicedradix.New(), keygen.Word, ftspreset.Multilingual())
```

Available presets:

- `ftspreset.English()`
- `ftspreset.Russian()`
- `ftspreset.Multilingual()`

Use `pkg/textproc` when you want an explicit pipeline:

```go
pipe := textproc.NewPipeline(
	textproc.AlnumTokenizer{},
	textproc.LowercaseFilter{},
	textproc.MinLengthOrNumericFilter{MinLength: 2},
	textproc.EnglishStopwordFilter{},
	textproc.EnglishStemFilter{},
)

engine := fts.New(slicedradix.New(), keygen.Word, fts.WithPipeline(pipe))
```

Each `fts.Field` can also override the service-level pipeline with its own `Field.Pipeline`.

Use a named pipeline when its output will be persisted in a sealed segment:

```go
pipe := textproc.NewNamedPipeline(
	"support-events",
	1,
	textproc.AlnumTokenizer{},
	textproc.LowercaseFilter{},
)
```

The name and version form a caller-owned compatibility identity. Increment the
version whenever the same input may produce different tokens. `NewPipeline(...)`
uses the generic identity `custom@0`, so prefer `NewNamedPipeline(...)` for
persisted indexes.

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
	keygen.Word,
	fts.WithPipeline(textproc.ObservabilityPipeline()),
)
```

The preset lowercases tokens, keeps numeric and two-character diagnostic terms,
and deliberately applies no stemming or stop-word removal. It also removes
duplicate tokens from each processed value. That makes it presence-oriented:
repeated terms do not increase term frequency, and repeated-token phrase
semantics are not preserved.

## Search API

Use:

- `SearchDocuments(...)` for query-string parsing
- `SearchPlainText(...)` for bag-of-words input without query syntax
- `SearchField(...)`, `SearchFields(...)` for field-scoped search
- `SearchPhrase(...)`, `SearchPhraseNear(...)` and field variants for phrase queries
- `SearchFieldClauses(...)` when different fields need different subqueries

Supported query-string syntax:

- `hotel`
- `french hotel`
- `"hotel barge"`
- `+hotel -market`
- `title:hotel`
- `title:"hotel barge"`
- `bar*`
- `+(title:hotel title:french) -market`

Programmatic queries are available through the AST types in `pkg/fts` such as `TermQuery`, `PhraseQuery`, `PrefixQuery`, and `BooleanQuery`.

Field behavior summary:

- with `fts.New(...)`, documents are indexed only into `_default`
- with `fts.NewMultiField(...)`, the service keeps a separate index per field name
- field indexes in multi-field mode are created lazily on first indexing of that field
- searching a field that has no index does not return an error; it returns zero matches

What that means for different search entry points:

- `SearchDocuments(...)` on a single-field service searches only `_default`
- `SearchDocuments(...)` on a multi-field service searches across the currently existing field indexes
- `SearchField(...)`, `SearchPhraseField(...)`, `SearchPhraseNearField(...)`, and field-scoped query syntax like `title:hotel` return zero matches when that field has never been indexed
- `SearchFields(...)`, `SearchPhraseFields(...)`, `SearchPhraseNearFields(...)`, and `SearchQueryFields(...)` search only the provided fields that currently exist; missing fields are ignored
- `SearchFieldClauses(...)` behaves the same way per clause: a clause targeting a missing field contributes no matches
- prefix search behaves the same with one extra rule: if the field exists but its index does not support prefix search, that field contributes no prefix matches
- phrase search behaves the same: if the field exists but its index does not implement `fts.PositionalIndex`, that field contributes no phrase matches

## Multi-Field Services

Use `fts.NewMultiField(...)` when documents have separate searchable fields:

```go
factory := func(string) (fts.Index, error) {
	return slicedradix.New(), nil
}

engine := fts.NewMultiField(factory, keygen.Word)

_ = engine.Index(context.Background(), fts.Document{
	ID: "doc-1",
	Fields: map[string]fts.Field{
		"title":    {Value: "French hotel"},
		"abstract": {Value: "Rosa runs hotel operations in France"},
	},
})

res, _ := engine.SearchField(context.Background(), "title", "hotel", 10)
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
	keygen.Word,
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
	keygen.Word,
	fts.WithScorer(fts.BM25()),
)
```

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

The recommended persistence surface for library consumers is `pkg/ftspersist`.

| Mode | Writable after load | Recommended API | Notes |
| --- | --- | --- | --- |
| snapshot | yes | `SaveSnapshot`, `LoadSnapshot` | restores a mutable service |
| segment | no | `SaveSegment`, `LoadSegment` | restores a sealed read-only service |

Important details:

- snapshot and segment formats are different and not interchangeable
- `mmap` is available only for segments via `ftspersist.SegmentLoadOptions{Access: ftspersist.AccessMmap}`
- `pkg/segment` is a lower-level API for raw segment files; prefer `pkg/ftspersist` unless you need direct segment access
- if you persist built-in indexes through snapshots, or built-in filters through snapshots or segments, call `ftsbuiltin.RegisterSnapshotCodecs()` once at startup

Sealed segment manifests store the service's default analyzer descriptor. Supply
the same pipeline when restoring the service. An optional expected fingerprint
can reject an incompatible segment before its field files are opened:

```go
pipe := textproc.ObservabilityPipeline()
descriptor := pipe.Descriptor()

loaded, err := ftspersist.LoadSegment(
	ftspersist.SegmentPaths{Dir: "./data/segments/events"},
	keygen.Word,
	ftspersist.SegmentLoadOptions{
		Access:                      ftspersist.AccessFile,
		ExpectedAnalyzerFingerprint: descriptor.Fingerprint,
	},
	fts.WithPipeline(pipe),
)
```

The compatibility gate currently covers sealed segments and the service-level
default pipeline. It does not describe mutable snapshots, field-level pipeline
overrides, or the key generator. Rebuild persisted data when any of those token
or key-generation rules change. Legacy v1 segment manifests remain readable but
do not contain analyzer metadata.

`AccessFile` keeps the term table resident and reads matching posting ranges from
the file. `AccessMmap` maps the segment. Opening either mode validates the segment
checksum, and callers must close the returned loaded service.

Current working persistence examples:

- `examples/client-library/snapshot-save-files/main.go`
- `examples/client-library/snapshot-load-files/main.go`
- `examples/client-library/snapshot-load-files-low-level/main.go`
- `examples/client-library/segment-save-files/main.go`
- `examples/client-library/segment-load-files/main.go`
- `examples/client-library/segment-load-files-low-level/main.go`
- `examples/client-library/segment-load-mmap/main.go`

See `examples/client-library/README.md` for the exact run order. The load examples expect artifacts created by the corresponding save examples.

## Diagnostics and Stats

Per-request diagnostics are opt-in:

```go
ctx := fts.WithDiagnostics(context.Background())
res, _ := engine.SearchDocuments(ctx, "postgres checkpoint", 10)

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
- `segment-analyzer-compatibility` - analyzer-compatible sealed segment restore
- `rank-profile` - multi-field ranking with weighted field scoring
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
