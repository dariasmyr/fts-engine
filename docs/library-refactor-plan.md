# Library-first refactor plan for `fts-engine`

## Goal

Make the project cleaner as a reusable Go library while keeping current behavior for CLI users.

## Current architecture notes

- Public API is in `pkg/*` and app-specific code is in `cmd/fts` + `internal/*`.
- This is generally correct for library usage.
- Main gaps for library-first design:
  - `pkg/fts` currently depends on `pkg/textproc` defaults, which pull in stemming dependencies.
  - Built-in registry/snapshot wiring lives in CLI (`cmd/fts/main.go`) and is not reusable from external projects.
  - CLI dependencies share the same module surface as the library.

## PR 1: Decouple `pkg/fts` core from heavy text deps

### Objective

Ensure importing `pkg/fts` does not implicitly bring stemming-related dependencies when users do not need them.

### Changes

1. Remove direct import of `pkg/textproc` from `pkg/fts/engine.go`.
2. Add lightweight default pipeline in `pkg/fts` (token split + lowercase or equivalent minimal behavior).
3. Keep advanced language presets in a dedicated public package (e.g. `pkg/ftspreset`), returning `fts.Option`.
4. Update README examples to show both:
   - zero-config core engine usage
   - opt-in advanced preset usage

### Files (expected)

- `pkg/fts/engine.go`
- `pkg/fts/default_pipeline.go` (new)
- `pkg/ftspreset/presets.go` (new)
- `readme.md`

### Validation

- `go test ./pkg/...`
- `go list -deps ./pkg/fts` (verify no stemming package in core dependency graph)

## PR 2: Expose built-in registration as reusable public helpers

### Objective

Allow external consumers to use built-in indexes/filters/snapshot codecs without copying CLI code.

### Changes

1. Move registration logic from `cmd/fts/main.go` into a public package (e.g. `pkg/ftsbuiltin`).
2. Expose explicit helpers:
   - `RegisterIndexes() error`
   - `RegisterFilters(opts FilterOptions) error`
   - `RegisterSnapshotCodecs() error`
3. Replace panic-based registration with error returns.
4. Update CLI to use these helpers.

### Files (expected)

- `pkg/ftsbuiltin/register.go` (new)
- `cmd/fts/main.go`
- `readme.md`

### Validation

- `go test ./...`
- smoke test: minimal external-style setup calling the public registration helpers

## PR 3 (optional but recommended): Reduce module dependency surface

### Objective

Keep library dependency graph lean and isolate CLI-specific dependencies.

### Options

1. **Soft option**: stay in one module, clearly document API stability scope (`pkg/*` only).
2. **Strong option**: split CLI into a nested module (or separate module) so `go get` for library users does not include CLI dependency set.

### Validation

- Library path: `go test ./pkg/...`
- CLI path: `go test ./...` and `go build ./cmd/fts`

## Rollout order

1. PR 1 (lowest risk, immediate library improvement)
2. PR 2 (better DX for external integrators)
3. PR 3 (organizational complexity, do after behavior is stable)

## Success criteria

- Importing `pkg/fts` stays lightweight.
- External project can configure built-ins without touching `cmd/fts` internals.
- CLI behavior remains unchanged for existing configs.
- Public API remains stable or is changed only with clear migration notes.
