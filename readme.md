# Full-Text Search Test Engine

Reusable full-text search engine in Go with configurable indexes, token pipeline, and snapshot support.

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

## Library usage

### 1) Install

```bash
go get github.com/dariasmyr/fts-engine@latest
```

If you test against local source:

```go
replace github.com/dariasmyr/fts-engine => /absolute/path/to/fts-engine
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

#### Custom `io.Writer`/`io.Reader` (with filter)

Use `pkg/ftsbuiltin` for built-in name-based codecs (`radix`, `bloom`, etc.) without manual codec registry wiring.

```go
package main

import (
	"bytes"
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func main() {
	opts := ftsbuiltin.FilterOptions{
		BloomExpectedItems: 1_000_000,
		BloomBitsPerItem:   10,
		BloomK:             7,
	}

	idx, _ := ftsbuiltin.BuildIndex("radix")
	flt, _ := ftsbuiltin.BuildFilter("bloom", opts)
	svc := fts.New(idx, keygen.Word, fts.WithFilter(flt))

	_ = svc.IndexDocument(context.Background(), "doc-1", "snapshot with bloom filter")

	var buf bytes.Buffer
	_ = ftsbuiltin.SaveServiceSnapshot(&buf, svc, "radix", "bloom")

	loaded, _ := ftsbuiltin.LoadSegmentSnapshot(bytes.NewReader(buf.Bytes()))
	restored := fts.New(loaded.Index, keygen.Word, fts.WithFilter(loaded.Filter))

	res, _ := restored.SearchDocuments(context.Background(), "snapshot", 10)
	fmt.Println(res.TotalResultsCount)
}
```

#### File-oriented variant (same snapshot payload)

```go
package main

import (
	"context"
	"os"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func main() {
	idx, _ := ftsbuiltin.BuildIndex("radix")
	svc := fts.New(idx, keygen.Word)
	_ = svc.IndexDocument(context.Background(), "doc-1", "file snapshot demo")

	out, _ := os.Create("./data/segments/default.fidx")
	defer out.Close()
	_ = ftsbuiltin.SaveServiceSnapshot(out, svc, "radix", "")

	in, _ := os.Open("./data/segments/default.fidx")
	defer in.Close()
	loaded, _ := ftsbuiltin.LoadSegmentSnapshot(in)

	_ = fts.New(loaded.Index, keygen.Word)
}
```

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

## Run main app (local testing via config)

Use this only when you want to test the repository app itself (`cmd/fts`), not when embedding the library into your service.

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
  engine: "trie"       # trie|kv
  index: "radix"       # radix|slicedradix|trigram|hamt|hamtpointered
  keygen: "word"       # word|trigram
  filter: "none"       # none|bloom|cuckoo
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
