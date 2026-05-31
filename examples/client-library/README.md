# Client library examples

This directory shows how to use `fts-engine` as a library from another Go project.

## Quick start from another repository

1. Create a new module:

```bash
mkdir my-fts-app
cd my-fts-app
go mod init example.com/my-fts-app
```

2. Add dependency:

```bash
go get github.com/dariasmyr/fts-engine@latest
```

3. Copy one of the examples below into your project and run:

```bash
go run .
```

If you test against local source, add a `replace` in your project's `go.mod`:

```go
replace github.com/dariasmyr/fts-engine => /absolute/path/to/fts-engine
```

## Examples in this folder

The examples currently cover three usage styles:

- in-memory library usage
- immutable segment export/restore
- custom runtime configuration

Current example list:

- `default/main.go` — minimal in-memory setup with defaults.
- `preset/main.go` — in-memory setup with language preset via `pkg/ftspreset`.
- `custom-options/main.go` — in-memory setup with custom pipeline and extra options.
- `snapshot-save-files/main.go` — save mutable snapshot files for a service created with `fts.New(...)`.
- `snapshot-load-files/main.go` — restore mutable snapshot files through the high-level `ftspersist.LoadSnapshot(...)` API.
- `snapshot-load-files-low-level/main.go` — restore mutable snapshot files through `LoadSnapshotData(...)` and assemble `fts.New(...)` manually.
- `segment-save-files/main.go` — export a sealed `segment` directory for a service created with `fts.New(...)`.
- `segment-load-files/main.go` — restore a sealed `segment` directory through the high-level `ftspersist.LoadSegment(...)` API.
- `segment-load-files-low-level/main.go` — restore a sealed `segment` directory through `LoadSegmentData(...)` and `RestoreSegmentService(...)` explicitly.

Run each example from repository root:

```bash
go run ./examples/client-library/default
go run ./examples/client-library/preset
go run ./examples/client-library/custom-options
go run ./examples/client-library/snapshot-save-files
go run ./examples/client-library/snapshot-load-files
go run ./examples/client-library/snapshot-load-files-low-level
go run ./examples/client-library/segment-save-files
go run ./examples/client-library/segment-load-files
go run ./examples/client-library/segment-load-files-low-level
```
