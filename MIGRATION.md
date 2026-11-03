# Migrating to v2

Version 2 introduces a deliberate breaking change to pipeline identity and
persistence metadata.

- Change imports from `github.com/dariasmyr/fts-engine/...` to
  `github.com/dariasmyr/fts-engine/v2/...`.
- Replace analyzer descriptors with `fts.PipelineDescriptor`.
- Implement `Fingerprint() string` on custom tokenizers and filters.
- Configure per-field pipelines with `fts.WithFieldPipelines`; `fts.Field` no
  longer carries a pipeline.
- Use package-provided `*fts.Option` values. Custom options and applying options
  after service construction are no longer supported.
- Rebuild v1 snapshots, segment manifests, and bundles. Loaders return
  `fts.ErrPersistenceRebuildRequired`, detectable with `errors.Is`.

Pipeline fingerprints must be self-describing, currently
`sha256:<64 lowercase hexadecimal characters>`, and must change whenever output
behavior changes.
