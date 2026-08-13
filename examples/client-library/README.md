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
go get github.com/dariasmyr/fts-engine@latest
```

3. Copy one of the examples below into your project and run:

```bash
go run .
```

If you want to test against local source, add a `replace` to your project's `go.mod`:

```go
replace github.com/dariasmyr/fts-engine => /absolute/path/to/fts-engine
```

## Example groups

Standalone examples:

- `default/main.go` - minimal in-memory setup with defaults
- `preset/main.go` - in-memory setup with a language preset from `pkg/ftspreset`
- `custom-options/main.go` - in-memory setup with a custom pipeline and filter
- `field-clauses/main.go` - field-specific query clauses
- `flat-observability/main.go` - flat index with observability-oriented tokenization
- `segment-analyzer-compatibility/main.go` - self-contained segment save and analyzer-compatible restore
- `rank-profile/main.go` - multi-field ranking with weighted field scoring

Snapshot examples:

- `snapshot-save-files/main.go` - save a mutable snapshot
- `snapshot-load-files/main.go` - restore it with the high-level `ftspersist.LoadSnapshot(...)` API
- `snapshot-load-files-low-level/main.go` - restore it with `LoadSnapshotData(...)` and assemble `fts.New(...)` manually

Segment examples:

- `segment-save-files/main.go` - export a sealed read-only segment directory
- `segment-load-files/main.go` - restore it with the high-level `ftspersist.LoadSegment(...)` API
- `segment-load-files-low-level/main.go` - restore it with `LoadSegmentData(...)` and `RestoreSegmentService(...)`
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
go run ./examples/client-library/segment-analyzer-compatibility
go run ./examples/client-library/rank-profile
```

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
go run ./examples/client-library/segment-load-files-low-level
go run ./examples/client-library/segment-load-mmap
```

Notes:

- the persistence examples write under `./data/segments`
- snapshot restore returns a writable service
- segment restore returns a read-only service
- `mmap` applies only to segment loading
- segment manifests store the default analyzer identity; restore them with the same named pipeline
