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

Recommended library flow is one combined snapshot payload with explicit codec registration.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func init() {
	_ = fts.RegisterIndexSnapshotCodec("slicedradix",
		func(index fts.Index, w io.Writer) error {
			s, ok := index.(fts.Serializable)
			if !ok {
				return fmt.Errorf("slicedradix: index does not implement Serializable")
			}
			return s.Serialize(w)
		},
		slicedradix.Load,
	)
}

func main() {
	svc := fts.New(slicedradix.New(), keygen.Word)
	_ = svc.IndexDocument(context.Background(), "doc-1", "snapshot demo")

	out, _ := os.Create("./data/segments/default.fidx")
	defer out.Close()
	_ = svc.SaveSnapshot(out, "slicedradix", "")

	in, _ := os.Open("./data/segments/default.fidx")
	defer in.Close()
	restored, _ := fts.NewFromSnapshot(in, keygen.Word)

	res, _ := restored.SearchDocuments(context.Background(), "snapshot", 10)
	fmt.Println(res.TotalResultsCount)
}
```

If you prefer built-in name-based helpers (`radix`, `bloom`, `ribbon`, etc.), use `pkg/ftsbuiltin` examples in `examples/client-library/snapshot` and `examples/client-library/snapshot-buffer-filter`.

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
  filter: "none"       # none|bloom|cuckoo|ribbon
  snapshot:
    enabled: true
    path: "./data/segments/default.fidx"
    split_files: true
    index_path: "./data/segments/local.index.fidx"
    filter_path: "./data/segments/loca.filter.fidx"
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
  ribbon:
    expected_items: 1000000
    extra_cells: 250000
    window_size: 24      # 1..32
    seed: 0
    max_attempts: 5
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
- `split_files`: if true, save/load index and filter in separate files.
- `index_path`: optional explicit path for index snapshot file in split mode.
- `filter_path`: optional explicit path for filter snapshot file in split mode.
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

## Ribbon filter usage

Ribbon is a static filter. In `fts` it is used via `BufferedStaticFilter`.

Preferred build API is stream-based (`BuildWithRetriesFromKeyStream`).

```go
opts := ftsbuiltin.FilterOptions{
	RibbonExpectedItems: 1_000_000, // estimated unique keys
	RibbonExtraCells:    250_000,   
	RibbonWindowSize:    16,       
	RibbonSeed:          0,       
	RibbonMaxAttempts:   5,
}

rf, _ := filter.NewRibbonFilter(
	opts.RibbonExpectedItems,
	opts.RibbonExtraCells,
	opts.RibbonWindowSize,
	opts.RibbonSeed,
)

stream := func(emit func([]byte) bool) error {
	keys := []string{"alpha", "hotel", "market"}
	for _, key := range keys {
		if !emit([]byte(key)) {
			break
		}
	}
	return nil
}

_ = rf.BuildWithRetriesFromKeyStream(stream, opts.RibbonMaxAttempts)

out, _ := os.Create("./data/segments/ribbon.filter.fidx")
defer out.Close()
_ = rf.Serialize(out)
```

If your keys come from files, add a thin adapter in client code that converts file parsing to stream emission.

Minimal parser adapter example (line-by-line keys):

```go
func parseKeysFile(path string, emit func([]byte) bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		key := strings.TrimSpace(s.Text())
		if key == "" {
			continue
		}
		if !emit([]byte(key)) {
			break
		}
	}

	return s.Err()
}
```

Load ribbon filter from file:

```go
in, _ := os.Open("./data/segments/ribbon.filter.fidx")
defer in.Close()

ribbonFilter, _ := filter.LoadRibbonFilter(in)

fmt.Println(ribbonFilter.Contains([]byte("market")))
```

Full runnable example (default parser save, custom parser save, load from file, normalized `Contains`) is in `examples/client-library/ribbon-file/main.go`.

### Standalone filter `Contains` with normalization

Use this when you store normalized keys in filter and later want to check a raw user word.

Example: indexed key is `beauty`, user enters `beautiful`.
With stemming, both become `beauti`, so normalized check returns `true`.

```go
pipe := textproc.NewPipeline(
	textproc.AlnumTokenizer{},
	textproc.LowercaseFilter{},
	textproc.EnglishStemFilter{},
)

indexedTerms := []string{"beauty", "hotel"}
normalizedKeys := make([]string, 0, len(indexedTerms))
for _, term := range indexedTerms {
	keys, _ := fts.NormalizeToKeys(term, pipe, keygen.Word)
	normalizedKeys = append(normalizedKeys, keys...)
}

rf, _ := filter.NewRibbonFilter(uint32(len(normalizedKeys)), 32, 24, 0)
stream := func(emit func([]byte) bool) error {
	for _, key := range normalizedKeys {
		if !emit([]byte(key)) {
			break
		}
	}
	return nil
}

_ = rf.BuildWithRetriesFromKeyStream(stream, 5)

raw := rf.Contains([]byte("beautiful")) // false: filter stores normalized keys

normalized, _ := fts.ContainsNormalized(rf, "beautiful", pipe, keygen.Word)

fmt.Println("raw", raw, "normalized", normalized) // raw=false normalized=true
```

`ContainsNormalized` applies pipeline + keygen and checks all normalized keys via `Contains`.

## Tests

Run all tests:

```bash
go test ./...
```

Run only public packages:

```bash
go test ./pkg/...
```
