# Refactor Plan: Refactoring FTS Engine into a Reusable Library

## TL;DR

- Move the project from an app-centric structure (`internal/...`) to a library-oriented architecture.
- Make indexer and filter implementations exportable.
- Keep two required CLI scenarios in `cmd/fts`: test-mode runs with configurable filter sets and `experiment` runs with performance measurements.
- Execute migration in phases with a controlled cutover.

## 1. Context and Problem

- Core search logic and indexers currently live in `internal/services/fts/*`; external projects cannot import them.
- The core API is mixed with application models and CLI-oriented flow.
- Adding new indexers and pipelines requires direct changes in `cmd/fts/main.go`.
- This makes reuse in forks and third-party services difficult.

## 2. Goals

- Provide an exportable, stable public API for:
  - search engine core;
  - indexers;
  - tokenization and filtering pipelines.
- Keep working CLI scenarios for:
  - test environment runs (engine with selected filter set);
  - `experiment` mode (performance metrics and analysis).
- Allow config evolution if a migration guide and compatible defaults are provided.
- Make the engine extensible via registry/factory without editing `main`.
- Add contract tests shared across all indexers.

### Non-goals

- Full CUI redesign.
- Replacing LevelDB in this RFC (only defining boundaries).

## 3. Design Principles

- Library-first: core does not depend on `internal` or UI/storage.
- Small interfaces: narrow interfaces and optional capability interfaces.
- Pluggable pipeline: tokenization/filters/keygen are dependencies.
- Deterministic behavior: stable result ordering.
- Clean cutover: direct migration to the new architecture without a legacy layer.

## 4. Target Package Structure

```text
.
├── cmd/
│   └── fts/                       # CLI entrypoint (test/experiment), no business logic
├── pkg/
│   ├── fts/                       # Public core API
│   │   ├── engine.go
│   │   ├── types.go
│   │   ├── registry.go
│   │   ├── options.go
│   │   └── stats.go
│   ├── textproc/                  # Tokenize + filter pipeline
│   │   ├── pipeline.go
│   │   ├── tokenizer.go
│   │   ├── filters/
│   │   │   ├── lowercase.go
│   │   │   ├── stopwords_en.go
│   │   │   ├── stem_en.go
│   │   │   ├── minlen.go
│   │   │   └── numeric_policy.go
│   ├── keygen/
│   │   ├── word.go
│   │   └── trigram.go
│   ├── index/
│   │   ├── slicedradix/
│   │   ├── radix/
│   │   ├── trigram/
│   │   ├── hamt/
│   │   └── hamtpointered/
│   └── persist/                   # Shared contracts (optional)
│       └── codec.go
├── internal/
│   ├── app/                       # CLI orchestration and wiring
│   ├── adapters/
│   │   ├── storage/leveldb/
│   │   ├── loader/wiki/
│   │   └── cui/
│   └── testsupport/               # Helpers for integration tests
└── config/
```

## 5. Public API (Proposed Contract)

```go
package fts

import "context"

type DocID string

type DocRef struct {
    ID    DocID
    Count uint32
}

type Result struct {
    ID            DocID
    UniqueMatches int
    TotalMatches  int
}

type SearchResult struct {
    Results           []Result
    TotalResultsCount int
    Timings           map[string]string
}

type Index interface {
    Insert(key string, id DocID) error
    Search(key string) ([]DocRef, error)
}

type Analyzer interface {
    Analyze() Stats
}

type Serializable interface {
    Serialize(w io.Writer) error
}

type IndexLoader func(r io.Reader) (Index, error)

type KeyGenerator func(token string) ([]string, error)

type Pipeline interface {
    Process(text string) []string
}

type Engine interface {
    IndexDocument(ctx context.Context, docID DocID, content string) error
    SearchDocuments(ctx context.Context, query string, maxResults int) (*SearchResult, error)
}
```

Notes:

- `DocID` in core stays `string` for compatibility with the current project and external systems.
- If `uint64` IDs are required, use an adapter in the calling project.

## 6. Registry/Factory for Indexers

- In `pkg/fts/registry.go`:
  - `RegisterIndex(name string, factory IndexFactory)`
  - `NewIndex(name string, opts ...Option) (Index, error)`
- Each index package may self-register in `init()`, or registration can be centralized in `internal/app/wiring`.
- CLI no longer owns a large `switch`; it resolves indexer by config key.

## 7. Tokenization and Filter Pipeline

- `pkg/textproc` provides one consistent path for both indexing and search.
- Default pipeline (`DefaultEnglishPipeline`) reproduces current behavior:
  1) tokenize (alnum split)
  2) lowercase
  3) English stopwords
  4) English stemming
  5) min-length policy
  6) numeric policy (configurable)
- For logs/code-search scenarios, policy can be swapped without core changes.

## 8. Preserving CLI Test and Experiment Scenarios

- `cmd/fts` must keep two mandatory run modes:
  - test mode: run engine with a configurable filter/pipeline set;
  - `experiment` mode: run with memory/time/index-structure measurements.
- `config` format may change (including new sections for pipeline/filters/bench) if:
  - a config migration guide is provided;
  - defaults allow quick local startup;
  - new fields are reflected in `config_local_example.yaml`.
- Base runtime flow remains:
  - load documents -> index -> search;
  - in `experiment`: collect and print metrics.
- Wiring changes: old constructors are replaced by public API + adapters.

## 9. Breaking Change Strategy (No Legacy Adapters)

- Migration moves directly to `pkg/*` architecture without an intermediate compatibility layer.
- Old packages in `internal/services/fts/*` are removed as the new equivalents are introduced.
- Changes are released as breaking:
  - release notes include explicit BREAKING marker;
  - `README` is updated to new import structure;
  - migration guide covers config and integration points.
- Mandatory cutover condition: test mode and `experiment` mode are verified before merge.

## 10. Migration Plan (PR-by-PR)

- PR-1: Core skeleton
  - Add `pkg/fts` types/interfaces/engine.
  - Do not remove anything yet.
  - Add unit tests for sorting/limits/timings.

- PR-2: Text pipeline
  - Add `pkg/textproc` + `pkg/keygen`.
  - Switch `pkg/fts.Engine` to pipeline dependency.
  - Add golden tests for tokenize/filter parity.

- PR-3: Public indexers
  - Example: move `slicedradix` to `pkg/index/slicedradix`.
  - Keep `Serialize/Load`.
  - Add contract + concurrency tests.

- PR-4: Registry + CLI wiring
  - Add `pkg/fts/registry`.
  - Switch `cmd/fts/main.go` to factory-based wiring.
  - Add configurable pipeline filters for test mode.

- PR-5: Remove old internal packages
  - Remove `internal/services/fts/*` once `pkg/*` equivalents exist.
  - Remove old wiring and unused types.

- PR-6: Internal boundaries
  - Extract `internal/adapters/storage/leveldb`, `internal/adapters/loader/wiki`, `internal/adapters/cui`.
  - Ensure core imports no `internal` packages.

- PR-7: Docs + examples + release
  - Update README for library usage with integration example.
  - Publish changelog + migration guide.

## 11. Testing Strategy

- Unit:
  - core ranking/limit/timing;
  - pipeline filters;
  - index-specific behavior.
- Contract suite:
  - one shared set executed against all indexers.
- Integration:
  - `cmd/fts` smoke tests for test mode and `experiment`.
- Regression (golden):
  - fixed dataset with expected top-N for key queries.
- Benchmark:
  - indexing/search performance before vs after refactor.

## 12. Acceptance Criteria (Definition of Done)

- External projects can import at minimum:
  - `pkg/fts`
  - one indexer (`pkg/index/slicedradix`)
  - `pkg/textproc`/`pkg/keygen`
- `go test ./...` is green (except explicitly marked temporary flaky tests).
- CLI supports test mode with configurable filters via config.
- `experiment` mode works and reports performance metrics after refactor.
- If config format changes, migration guide exists and `config/config_local_example.yaml` is updated.
- Repository has no legacy adapters and no duplicate old implementations in `internal/services/fts/*`.
- Documentation includes:
  - quick start for library mode;
  - migration guide for previous imports.
- Performance does not degrade by >10% without explicit justification.

## 13. Example Usage in a Third-Party Project (Target DX)

```go
idx := slicedradix.New()
pipe := textproc.DefaultEnglishPipeline()
eng := fts.NewEngine(idx, keygen.Word, pipe)

_ = eng.IndexDocument(ctx, "doc-1", "Connection refused on 10.0.0.1")
res, _ := eng.SearchDocuments(ctx, "refused connection", 10)
_ = res
```
