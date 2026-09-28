# Architecture Cleanup

Этот файл фиксирует технический backlog по организации vector и persistence
кода. Это не список новых функций: изменения должны сохранять текущее
поведение и wire format.

## Current Model

### Prepared vectors

`vectorstore.PreparedVectorStore` описывает неизменяемый random-access storage для
подготовленных vector rows:

- `Len`, `Dimensions`, `Metric`, `Normalization` описывают layout;
- `ReadVectorInto` читает одну row по ordinal;
- реализация не должна удерживать переданный destination buffer;
- context должен поддерживать cancellation.

Основная production-реализация: `vectorstore.MemoryVectorStore`.

`benchmarks/internal/vectorsearch/flat.VectorStore` является отдельной
benchmark-реализацией. Она нужна flat oracle, потому что ему необходим прямой
доступ к contiguous matrix, exact scan и duplicate statistics. Это не вторая
production-реализация.

Test doubles (`testSource`, `graphPreparedSource` и подобные) не считаются
архитектурными реализациями.

### Interfaces

Правило размещения интерфейсов:

- consumer-side интерфейсы объявлять в consumer package;
- общий интерфейс оставлять в `pkg/vector`, только если он является частью
  публичного контракта нескольких индексов;
- не создавать отдельный интерфейс для каждого конкретного типа;
- не дублировать одинаковые production-интерфейсы без необходимости.

`PreparedVectorStore` является общим контрактом для `hnsw`, `semantic`,
`semanticpersist` и flat benchmark adapter, поэтому находится в `pkg/vectorstore`.

## Target Architecture

### Vector Core

`pkg/vector` owns only shared vector mathematics and search protocol types:

```text
pkg/vector/
  doc.go
  errors.go
  metric.go
  metric_types.go
  ordinal.go
  search_types.go
```

It must not own HNSW topology, flat storage, or filesystem persistence.

### Vector Storage

Prepared vector storage moves to a dedicated package:

```text
pkg/vectorstore/
  store.go
  memory.go
```

`pkg/vectorstore` owns `PreparedVectorStore`, `MemoryVectorStore`, and storage
specific validation. It depends on `pkg/vector`; `pkg/vector` does not depend on
it.

### HNSW

```text
pkg/vector/hnsw/
  config.go
  topology.go
  builder.go
  index.go
  search.go
  validation.go
  format_adapter.go
  internal/format/
    format.go
    encode.go
    decode.go
```

Responsibilities:

- `builder.go` builds graphs;
- `index.go` owns the immutable HNSW index;
- `search.go` executes graph traversal;
- `topology.go` owns private packed topology;
- `validation.go` validates HNSW semantics;
- `format_adapter.go` translates private HNSW values to format DTOs;
- `internal/format/` owns only the VHNG wire format. Its bounded framing and
  checksum helpers are shared through the repository-level `internal/format`.

The public vector contract is named `Index`, while `HNSWIndex` remains the
concrete immutable HNSW implementation.

### Semantic Layer

```text
pkg/semantic/
  types.go
  segment.go
  search.go
  read_view.go
  compaction.go
  service.go
```

This layer owns semantic segments, document/vector mappings, read views,
compaction, and search orchestration. It must not know filesystem filenames or
individual persistence codecs.

### Semantic Persistence

```text
pkg/semanticpersist/
  api.go
  paths.go
  publish.go
  object.go
  generation.go
  current.go
  recovery.go
  internal/format/
    vectors.go
    state.go
    manifest.go
    current.go
```

`object.go` owns immutable objects, `generation.go` owns generation directories,
`current.go` owns the atomic publication pointer, and `recovery.go` owns repair
and recovery. Persistence codecs stay isolated from publication orchestration.
Semantic schemas remain separate from VHNG, while bounded binary framing and
checksum helpers are shared through the repository-level `internal/format`.

### Interfaces

Do not create `interfaces` or `common` packages. Shared interfaces live in a
small package only when they are part of a public cross-package contract.
Consumer-specific interfaces stay next to their consumer. Interfaces should be
minimal and describe the boundary they serve.

### Benchmarks

Benchmarks keep their implementation-specific names and do not define the
production architecture:

```text
benchmarks/internal/vectorsearch/
  flat/
    index.go
    flat_index.go
    store.go
  hnsw/
  truth/
  report/
```

`FlatIndex` and `VectorStore` describe the implementation. `exactTruth` may be
used only for the role of reference results.

## Refactoring Backlog

### High priority

- [x] Перенести `PreparedVectorStore` и `MemoryVectorStore` из `pkg/vector` в
  `pkg/vectorstore`, затем убрать storage implementation details из vector core.
- Проверить, что production-код использует имя `vectors` или `vectorStore`, а
  не расплывчатое `source`, когда речь идет именно о prepared vector storage.
- Усилить документацию `PreparedVectorStore`: требования к размеру `dst`,
  диапазону ordinal, prepared/normalized данным, ownership и cancellation.
- Проверить `NewPreparedMemoryVectorStore`: либо валидировать каждую строку при
  создании, либо явно документировать, что caller гарантирует prepared data.
- Добавить прямые package tests для внутреннего VHNG format package, а не проверять
  DTO и VHNG codec только через `hnsw` adapter.
- Проверить, что `pkg/vector/hnsw/graph_format.go` содержит только адаптацию
  HNSW topology и semantic validation, а wire-format код остается в
  его через публичный путь.

### Medium priority

- Решить, нужен ли более точный интерфейсный термин `PreparedVectorReader`
  вместо `PreparedVectorStore`. Переименование делать только если storage
  semantics больше не является целевой абстракцией.
- Рассмотреть разделение общего storage contract на меньшие consumer-side
  interfaces (`VectorLayout` и row reader) только после переноса в
  `pkg/vectorstore` и только при наличии consumers, которым действительно
  нужен только один из этих аспектов.
- Проверить публичный `vector.Index`: оставить его как общий контракт flat
  и HNSW индексов или сделать benchmark-only interface локальным в benchmarks.
- Уменьшить DTO conversion boilerplate между `hnsw.HNSWIndex` и
  `hnsw/format.Graph`, не раскрывая приватную HNSW topology.
- Разнести оставшуюся semantic validation в `pkg/vector/hnsw` по файлам, если
  `graph_format.go` снова начнет расти.

### Low priority

- Унифицировать terminology в documentation и benchmark report labels:
  `FlatIndex`, `VectorStore`, `HNSW index`.
- Проверить имена локальных benchmark helpers и не смешивать implementation
  name (`flat`) с algorithm role (`exact truth`).
- Добавить package-level documentation для внутреннего VHNG format package с описанием
  DTO ownership и допустимых границ валидации.

## Non-goals

- Не добавлять backward-compatibility aliases для удаленных неоднозначных имен.
- Не переносить production `MemoryVectorStore` в benchmark packages.
- Не объединять flat benchmark storage с HNSW topology.
- Не менять `VFLT`, `VHNG`, `SSTA`, `SMAN` или `SCUR` wire formats.
- Не менять публичное поведение поиска, публикации поколений или recovery.

## Verification

Каждый structural refactor должен проходить:

```text
go build ./...
go vet ./...
go test ./...
go test ./...        # from benchmarks/
```

Для persistence и graph changes дополнительно проверять golden bytes,
truncation/corruption tests и lifecycle benchmarks.
