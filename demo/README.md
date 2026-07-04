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
