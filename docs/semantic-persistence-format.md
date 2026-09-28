# Semantic Persistence Format

Semantic persistence is composed from immutable files with separate owners.
The files are linked by size and SHA-256 references; they are never updated in
place.

| File | Magic | Owner | Content | Write/read entry points |
| --- | --- | --- | --- | --- |
| `vectors.bin` | `VFLT` | `semanticpersist` | Prepared dense `float32` rows | `WriteVectorFile` / `OpenVectorFile` |
| `graph.bin` | `VHNG` | `vector/hnsw` | Packed HNSW topology and search configuration | `WriteGraphFile` / `OpenGraphFile` |
| `semantic-state.bin` | `SSTA` | `semanticpersist` | Descriptors, vector IDs, chunk references, and limits | `encodeState` / `decodeState` |
| `manifest.bin` | `SMAN` | `semanticpersist` | Generation metadata and file references | `encodeManifest` / `decodeManifest` |
| `CURRENT` | `SCUR` | `semanticpersist` | Active generation ID and manifest hash | `encodeCurrent` / `decodeCurrent` |

## Immutable Objects

Published stores keep vector and graph files in a content-addressed segment
object:

```text
objects/segments/<object-id>/
  vectors.bin
  graph.bin
```

The object ID is derived from the segment kind and the vector and graph
references. An identical object can be reused by multiple generations.

## Generations

A generation binds one immutable object to semantic state:

```text
generations/<generation-id>/
  semantic-state.bin
  manifest.bin
```

`CURRENT` is the commit pointer. A publication writes and validates the object
and generation first, then atomically replaces `CURRENT` as the final commit
step. Readers continue using the previous generation until that replacement.

## Shared Codec

`internal/format` contains only bounded binary framing primitives: headers,
little-endian values, strings, limits, and decoder errors. The VHNG wire format
itself is implemented by the HNSW package's internal VHNG format codec, which
exposes DTOs instead of
depending on HNSW's private topology. Neither package knows about generations
or semantic metadata. Each format retains its own magic, version, validation,
and payload schema.
