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
)

func main() {
	idx := radix.New()
	engine := fts.New(idx, keygen.Word)

	_ = engine.IndexDocument(context.Background(), "doc-1", "Wikipedia: Rosa is a French hotel barge")
	res, _ := engine.SearchDocuments(context.Background(), "french hotel", 10)

	fmt.Println(res.TotalResultsCount)
}
```

## Segment snapshots (append-only)

Index/filter state can be persisted to any `io.Writer` (file, object storage stream) and restored from `io.Reader`.

```go
var buf bytes.Buffer

// Register snapshot codecs for selected index/filter once.
// Built-in helpers are available in pkg/ftsbuiltin.

svc := fts.New(radix.New(), keygen.Word)
_ = svc.IndexDocument(context.Background(), "doc-1", "hello world")
_ = svc.SaveSnapshot(&buf, "radix", "")

restored, _ := fts.NewFromSnapshot(bytes.NewReader(buf.Bytes()), keygen.Word)
res, _ := restored.SearchDocuments(context.Background(), "hello", 10)
fmt.Println(res.TotalResultsCount)
```

### File snapshots: default and configurable modes

`SaveSnapshotFile` uses default file persistence mode: atomic publish (`tmp -> rename`), batched buffered writes, and file sync enabled.

Default mode example (save + load + search):

```go
svc := fts.New(radix.New(), keygen.Word)
_ = svc.IndexDocument(context.Background(), "doc-1", "hello world")

_ = svc.SaveSnapshotFile("./data/segments/default.fidx", "radix", "")

loaded, _ := fts.NewFromSnapshotFile("./data/segments/default.fidx", keygen.Word)
res, _ := loaded.SearchDocuments(context.Background(), "hello", 10)
fmt.Println(res.TotalResultsCount)
```

Configurable mode example (custom buffer policy):

```go
opts := fts.DefaultSnapshotFileOptions()
opts.BufferSize = 2 << 20      // 2 MiB
opts.FlushThreshold = 512 << 10 // 512 KiB
opts.SyncFile = true

svc := fts.New(radix.New(), keygen.Word)
_ = svc.IndexDocument(context.Background(), "doc-1", "hello world")

_ = svc.SaveSnapshotFileWithOptions("./data/segments/default.fidx", "radix", "", opts)
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

### 2) Choose one initialization flow

#### Quickstart

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
	idx := radix.New()
	engine := fts.New(idx, keygen.Word)

	_ = engine.IndexDocument(context.Background(), "doc-1", "Wikipedia: Rosa is a French hotel barge")
	res, _ := engine.SearchDocuments(context.Background(), "french hotel", 10)

	fmt.Println(res.TotalResultsCount)
}
```

Use direct constructors and start indexing immediately.

```go
engine := fts.New(radix.New(), keygen.Word)
```

#### Explicit flow (recommended for most clients)

Pick the exact index/key generator pair your app needs and keep wiring explicit.

- Word index: `radix.New()` + `keygen.Word`
- Trigram index: `trigram.New()` + `keygen.Trigram`

```go
idx := trigram.New()
engine := fts.New(idx, keygen.Trigram, fts.WithPipeline(textproc.DefaultEnglishPipeline()))
```

#### Built-in registry flow (config-driven by name)

Use `pkg/ftsbuiltin` only when your app builds components by string name from config.

```go
_ = ftsbuiltin.RegisterIndexes()
_ = ftsbuiltin.RegisterFilters(ftsbuiltin.FilterOptions{
	BloomExpectedItems: 1_000_000,
	BloomBitsPerItem:   10,
	BloomK:             7,
	CuckooBucketCount:  262144,
	CuckooBucketSize:   4,
	CuckooMaxKicks:     500,
})
_ = ftsbuiltin.RegisterSnapshotCodecs()

idx, _ := fts.NewIndex("radix")
flt, _ := fts.NewFilter("bloom")
engine := fts.New(idx, keygen.Word, fts.WithFilter(flt))
```

If you do not construct by name from config, skip built-in registration.

Available defaults:

- `textproc.DefaultEnglishPipeline()`
- `textproc.DefaultRussianPipeline()`
- `textproc.DefaultMultilingualPipeline()`

You can also apply language presets as options via `pkg/ftspreset`:

```go
engine := fts.New(radix.New(), keygen.Word, ftspreset.English())
```

### 3) Optional: custom filter pipeline

```go
pipe := textproc.NewPipeline(
	textproc.AlnumTokenizer{},
	textproc.LowercaseFilter{},
	textproc.MinLengthOrNumericFilter{MinLength: 2},
)
engine := fts.New(radix.New(), keygen.Word, fts.WithPipeline(pipe))
```

### 4) Optional: filter-only usage (without index)

You can use probabilistic filters as standalone membership checks.

```go
bf := filter.NewBloomFilter(1_000_000, 10, 7)
bf.Add([]byte("alice@example.com"))

fmt.Println(bf.Contains([]byte("alice@example.com"))) // true
fmt.Println(bf.Contains([]byte("bob@example.com")))   // usually false, possible false positive

cf := filter.NewCuckooFilter(262144, 4, 500)
ok := cf.Add([]byte("session:123"))
fmt.Println(ok, cf.Contains([]byte("session:123")))
```

Notes:

- Bloom filter: `Add` always returns `true`; supports false positives.
- Cuckoo filter: `Add` may return `false` when the filter is saturated.
- Filter-only mode does not return documents; use an index for full-text retrieval.

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
  snapshot:
    enabled: true
    path: "./data/segments/default.fidx"
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
- `path`: final snapshot artifact path.
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

## Tests

Run all tests:

```bash
go test ./...
```

Run only public packages:

```bash
go test ./pkg/...
```
