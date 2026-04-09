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

Recommended path first:

- `default/main.go` — minimal setup with defaults.
- `snapshot/main.go` — save/load one snapshot payload in library mode.
- `preset/main.go` — language preset via `pkg/ftspreset`.

Additional scenarios:

- `custom-options/main.go` — custom pipeline and extra options.
- `filters-dynamic/main.go` — built-in dynamic filters (`bloom`, `cuckoo`).
- `filter-ribbon/main.go` — built-in static `ribbon` filter + explicit finalize.
- `snapshot-buffer-filter/main.go` — in-memory snapshot (`io.Writer`/`io.Reader`) with Bloom filter.
- `ribbon-file/main.go` — advanced ribbon flow with file parsing adapters.

Run each example from repository root:

```bash
go run ./examples/client-library/default
go run ./examples/client-library/preset
go run ./examples/client-library/custom-options
go run ./examples/client-library/snapshot
go run ./examples/client-library/snapshot-buffer-filter
go run ./examples/client-library/filters-dynamic
go run ./examples/client-library/filter-ribbon
go run ./examples/client-library/ribbon-file
```
