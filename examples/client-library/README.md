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

- `default/main.go` — minimal setup with defaults.
- `preset/main.go` — language preset via `pkg/ftspreset`.
- `custom-options/main.go` — custom pipeline and extra options.
- `snapshot/main.go` — save/load snapshot in pure library mode.

Run each example from repository root:

```bash
go run ./examples/client-library/default
go run ./examples/client-library/preset
go run ./examples/client-library/custom-options
go run ./examples/client-library/snapshot
```
