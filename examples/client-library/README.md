# Client library examples

This directory shows how to use `fts-engine` as a library from another Go project.

## Quick start from another repository

1. Create a new module:

```bash
mkdir my-fts-app
cd my-fts-app
go mod init example.com/my-fts-app
```

2. Add the dependency:

```bash
go get github.com/dariasmyr/fts-engine/v2@latest
```

3. Copy one of the examples below into your project and run:

```bash
go run .
```

If you want to test against local source, add a `replace` to your project's `go.mod`:

```go
replace github.com/dariasmyr/fts-engine/v2 => /absolute/path/to/fts-engine
```

## Example groups

Standalone examples:

- `default/main.go` - minimal in-memory setup with defaults
- `preset/main.go` - in-memory setup with a language preset from `pkg/ftspreset`
- `custom-options/main.go` - in-memory setup with a custom pipeline and filter
- `field-clauses/main.go` - field-specific query clauses
- `flat-observability/main.go` - flat index with observability-oriented tokenization
- `filtered-search/main.go` - high-level candidate filtering through `fts.SearchOptions`
- `filtered-presence/main.go` - low-level Amber-style FPR1 search with store-owned IDs, a candidate bitmap and streaming intersection
- `segment-pipeline-compatibility/main.go` - self-contained segment save and pipeline-compatible restore
- `segment-bundle/main.go` - save and restore one sealed segment as an `io.Writer`/`io.Reader` blob
- `rank-profile/main.go` - multi-field ranking with weighted field scoring
- `semantic-commit/main.go` - mutable document search and explicit `Commit` visibility
- `semantic-persistence/main.go` - `service.New`, `CommitAndSaveService`, `Inspect`, mismatch rejection, and `service.Load`
- `semantic-index-persistence/main.go` - raw `Index` persistence with `CommitAndSave`, `Load`, and request-local `QueryConfig`
- `semantic-ollama/main.go` - external Ollama embeddings with separate document and query prompts

Snapshot examples:

- `snapshot-save-files/main.go` - save a mutable snapshot
- `snapshot-load-files/main.go` - restore it with the high-level `ftspersist.LoadSnapshot(...)` API
- `snapshot-load-files-low-level/main.go` - restore it with `LoadSnapshotData(...)` and assemble `fts.New(...)` manually

Segment examples:

- `segment-save-files/main.go` - export a sealed read-only segment directory
- `segment-load-files/main.go` - restore it with the high-level `ftspersist.LoadSegment(...)` API
- `segment-load-mmap/main.go` - restore it with `ftspersist.LoadSegment(...)` using `mmap`

## Running from this repository

Run examples from repository root.

Standalone examples can be run independently:

```bash
go run ./examples/client-library/default
go run ./examples/client-library/preset
go run ./examples/client-library/custom-options
go run ./examples/client-library/field-clauses
go run ./examples/client-library/flat-observability
go run ./examples/client-library/filtered-search
go run ./examples/client-library/filtered-presence
go run ./examples/client-library/segment-pipeline-compatibility
go run ./examples/client-library/segment-bundle
go run ./examples/client-library/rank-profile
go run ./examples/client-library/semantic-commit
go run ./examples/client-library/semantic-persistence
go run ./examples/client-library/semantic-index-persistence
```

The Ollama example additionally requires a running model:

```bash
ollama pull nomic-embed-text
OLLAMA_MODEL=nomic-embed-text:latest \
OLLAMA_DIMENSIONS=768 OLLAMA_CONTEXT_TOKENS=2048 \
  go run ./examples/client-library/semantic-ollama
```

The client obtains the immutable model digest from Ollama automatically. It
also verifies dimensions, context capacity, and embedding capability before
accepting requests. The resolved digest is available through
`client.Model().Revision` and is used for semantic compatibility metadata.

Snapshot restore examples depend on files created by the snapshot save example:

```bash
go run ./examples/client-library/snapshot-save-files
go run ./examples/client-library/snapshot-load-files
go run ./examples/client-library/snapshot-load-files-low-level
```

Segment restore examples depend on files created by the segment save example:

```bash
go run ./examples/client-library/segment-save-files
go run ./examples/client-library/segment-load-files
go run ./examples/client-library/segment-load-mmap
```

Notes:

- the persistence examples write under `./data/segments`
- snapshot restore returns a writable service
- segment restore returns a read-only service
- `mmap` applies only to segment loading
- snapshots and segment manifests store per-field pipeline fingerprints; restore them with identically configured default and field pipelines
