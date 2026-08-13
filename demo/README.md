# demo

Interactive terminal demo app for `fts-engine`.

This module is intended to run inside the repository root workspace defined by `../go.work`. Its `go.mod` also keeps a local `replace ../` fallback so module-local commands keep resolving the root library module.

## Run

From repository root:

```bash
(cd demo && go run ./cmd/fts)
```

With an explicit config file:

```bash
(cd demo && go run ./cmd/fts -config ./config/config_local.yaml)
```

## Config

- default config path: `./config/config_local.yaml`
- example config: `./config/config_local_example.yaml`

The app depends on the public library module at `github.com/dariasmyr/fts-engine/pkg/...` and keeps its app-specific code inside `demo/internal/*`.

Relevant FTS choices:

- `fts.index`: `slicedradix`, `hamt`, or `flat`
- `fts.pipeline.preset`: `custom` or `observability`
- custom pipeline filter settings apply only when `preset: custom`

For an observability-oriented run, select:

```yaml
fts:
  index: "flat"
  pipeline:
    preset: "observability"
```

Keep the default `custom` preset for the bundled Wikipedia corpus. When changing
the index or pipeline, use a fresh persistence path or rebuild persisted data.
Sealed segments validate the default analyzer identity; mutable snapshots do not
currently store it.

Demo segments created before analyzer-aware custom configuration used the generic
identity `custom@0`. Rebuild them once after this update; the demo now derives a
distinct identity from every token-output setting.
