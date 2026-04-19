# Full-Text Search Engine

Reusable full-text search engine in Go with configurable indexes, filters, stemming pipeline, and snapshot support.

<p align="center"><img src="docs/logo.jpg" alt="Logo" width="50%"></p>

![Demo](docs/demo.gif)

## What this repository provides

- Public library API in `pkg/fts`.
- Public index implementations in `pkg/index/*`:
  - `radix`
  - `slicedradix`
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

Index and filter snapshots are always stored in separate files.

Flow 1 (recommended): manual codec registration via `init()` + split files.

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
	idx, _ := svc.SnapshotComponents()

	// export to file
	idxOut, _ := os.Create("./data/segments/default.index.fidx")
	defer idxOut.Close()
	_ = fts.SaveIndexSnapshot(idxOut, "slicedradix", idx)

	
	// open from file
	idxIn, _ := os.Open("./data/segments/default.index.fidx")
	defer idxIn.Close()
	loadedIndex, _ := fts.LoadIndexSnapshot(idxIn)
	restored := fts.New(loadedIndex.Index, keygen.Word)

	res, _ := restored.SearchDocuments(context.Background(), "snapshot", 10)
	fmt.Println(res.TotalResultsCount)
}
```

Flow 2: ready-to-use built-in codecs and filters is now in examples:
- `examples/client-library/snapshot-save-files/main.go`
- `examples/client-library/snapshot-import-files/main.go`

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

Download the Wikipedia dump from:

`https://archive.org/download/enwiki-20210820`

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
  engine: "trie"
  index: "radix"       # radix|slicedradix|hamt|hamtpointered
  keygen: "word"
  filter: "none"       # none|bloom|cuckoo|ribbon
  snapshot:
    enabled: true
    path: "./data/segments/default.fidx"
    index_path: "./data/segments/local.index.fidx"
    filter_path: "./data/segments/local.filter.fidx"
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
- `path`: base path used to derive split files when explicit paths are not set (`*.index.*` and `*.filter.*`).
- `index_path`: optional explicit path for index snapshot file.
- `filter_path`: optional explicit path for filter snapshot file.
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
expectedItems := uint32(1_000_000) // estimated unique keys
extraCells := uint32(250_000)
windowSize := uint32(16)
seed := uint64(0)
maxAttempts := uint32(5)

rf, _ := filter.NewRibbonFilter(
	expectedItems,
	extraCells,
	windowSize,
	seed,
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

_ = rf.BuildWithRetriesFromKeyStream(stream, maxAttempts)

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
