# Full-Text Search Test Engine 

Reusable full-text search engine in Go with configurable indexing strategies, token pipeline, CLI mode, and experiment mode.

![Demo](docs/demo.gif)

## What this repository provides

- Public library API in `pkg/fts`.
- Public index implementations in `pkg/index/*`:
  - `radix`
  - `slicedradix`
  - `trigram`
  - `hamt`
  - `hamtpointered`
- Public text processing pipeline in `pkg/textproc`.
- Public key generators in `pkg/keygen`.
- Public probabilistic filters in `pkg/filter`.
- CLI entrypoint in `cmd/fts` with:
  - `prod` mode (run with configurable filters and interactive CUI)
  - `experiment` mode (collect indexing metrics)

## Quick start (library mode)

```go
package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/radix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
	"github.com/dariasmyr/fts-engine/pkg/textproc"
)

func main() {
	idx := radix.New()
	pipe := textproc.DefaultEnglishPipeline()
	engine := fts.New(idx, keygen.Word, fts.WithPipeline(pipe))

	_ = engine.IndexDocument(context.Background(), "doc-1", "Wikipedia: Rosa is a French hotel barge")
	res, _ := engine.SearchDocuments(context.Background(), "french hotel", 10)

	fmt.Println(res.TotalResultsCount)
}
```

## Usage in a third-party project

This example shows how to consume the engine as a library from another Go service.

### 1) Add dependency

In your project:

```bash
go get github.com/dariasmyr/fts-engine@latest
```

If you work from a local checkout, use `replace` in `go.mod`:

```go
replace github.com/dariasmyr/fts-engine => /absolute/path/to/fts-engine
```

### 3) Switch strategy without changing app flow

- Word index: `radix.New()` + `keygen.Word`
- Trigram index: `trigram.New()` + `keygen.Trigram`

Example:

```go
idx := trigram.New()
engine := fts.New(idx, keygen.Trigram, fts.WithPipeline(textproc.DefaultEnglishPipeline()))
```

Available defaults:

- `textproc.DefaultEnglishPipeline()`
- `textproc.DefaultRussianPipeline()`
- `textproc.DefaultMultilingualPipeline()`

### 3) Optional: custom filter pipeline

```go
pipe := textproc.NewPipeline(
	textproc.AlnumTokenizer{},
	textproc.LowercaseFilter{},
	textproc.MinLengthOrNumericFilter{MinLength: 2},
)
engine := fts.New(radix.New(), keygen.Word, fts.WithPipeline(pipe))
```

## Build and run (CLI)

Install dependencies:

```bash
go mod tidy
```

Build:

```bash
go build -o build/fts ./cmd/fts
```

Run:

```bash
./build/fts --config=./config/config_local.yaml
```

## Configuration

Main file: `config/config_local.yaml` (create from `config/config_local_example.yaml`).

Core fields:

```yaml
fts:
  engine: "trie"
  index: "radix"      # radix|slicedradix|trigram|hamt|hamtpointered
  keygen: "word"      # word|trigram
  filter: "none"      # none|bloom|cuckoo
  bloom:
    expected_items: 1000000
    bits_per_item: 10
    k: 7
  cuckoo:
    bucket_count: 262144
    bucket_size: 4
    max_kicks: 500
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

## CLI modes

- `prod`: run engine with configurable pipeline and interactive CUI search.
- `experiment`: run indexing and print memory/index stats.

## Tests

Run all tests:

```bash
go test ./...
```

Run only public packages:

```bash
go test ./pkg/...
```
