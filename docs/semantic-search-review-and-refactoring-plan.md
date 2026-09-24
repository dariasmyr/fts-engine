# Semantic Search: Review And Refactoring Plan

## Статус документа

Документ описывает состояние semantic search в текущей ветке, сравнивает его с
архитектурой `pkg/fts`, `pkg/textproc`, `pkg/segment`, `pkg/ftspersist` и
`pkg/vector`, и предлагает целевую реализацию вместо текущего lifecycle.

Ревью выполнено по состоянию текущего worktree. Документ является живым планом:
статусы ниже сверены с текущим кодом, а не с первоначальным состоянием ветки.
Проверки,
выполненные перед подготовкой документа:

- `go test ./pkg/...` — успешно;
- `go test ./...` — успешно;
- `go run ./tools/depcheck` — успешно;
- gopls diagnostics для semantic, vector и semantic persistence — без ошибок.

Успешные тесты подтверждают внутреннюю согласованность текущей реализации, но
не подтверждают, что выбранная архитектура соответствует назначению semantic
search. Часть тестов прямо закрепляет поведение, которое в этом документе
предлагается удалить: exact fallback и обязательный путь через snapshot.

Текущая реализация прошла большую часть runtime refactoring path: runtime уже
использует immutable HNSW segments, service агрегирует component-local queries,
`Flush` строит segment вне `Service.mu` и публикует его через короткий generation
swap, а `Compact` выполняет HNSW merge. Persistence поддерживает HNSW-only
artifact. Незавершёнными остаются segment identity/source fingerprint,
проверка descriptor compatibility и полноценное разделение
Snapshot/Segment/Generation. Оставшиеся низкоуровневые exact/fallback
примитивы находятся в `pkg/vector` как benchmark/reference tools и не входят в
semantic runtime.

## Итоговая рекомендация

Текущую semantic-ветку лучше не продолжать как есть. Нужен контролируемый
рефакторинг с заменой lifecycle, а не косметическое переименование `flat` в
`HNSW`.

Целевая модель:

```text
caller embeddings
        |
        v
mutable vector store / ingest head
        |
        | seal / flush / merge
        v
immutable HNSW segment(s) + vector source + chunk rows
        |
        | query every visible segment, merge top candidates
        v
chunk hits -> document grouping

optional persistence:
  mutable state checkpoint = recoverable ingest state
  sealed segment           = ready-to-search immutable ANN artifact
  read view                = coherent immutable view over segments
  generation store         = optional atomic publication/retention layer
```

Ключевые решения:

- `flat` не участвует в semantic search ни как основной индекс, ни как
  fallback; его можно оставить для benchmark и для низкоуровневого codec/source
  при необходимости;
- HNSW является физическим поисковым индексом semantic search, а не опцией,
  наложенной на `ReadView`;
- vector storage и graph topology разделяются, но принадлежат одной immutable
  segment и связываются проверяемым fingerprint/identity;
- `ReadView` и `SealedSegment` имеют разные ответственности, как в lexical
  search;
- disk persistence остаётся опциональным и не является условием обычного
  in-memory ANN search;
- tokenizer не используется для embedding search. Для semantic нужны отдельные
  descriptors модели embedding и chunker-а; lexical analyzer остаётся частью
  `pkg/fts`.

## Naming Decision

В semantic lifecycle слово `Snapshot` больше не используется как общее название
для любого immutable объекта. Термины имеют разные обязанности:

```text
SemanticIndex / Service
  mutable coordinator: ingest, mappings, tombstones, visible components

SealedSegment
  один immutable физический ANN-компонент:
  vector source + HNSW topology + vector rows + segment metadata

ReadView
  immutable логическое представление набора visible SealedSegment-ов
  на определённой generation/version

MutableStateCheckpoint
  optional persistence mutable ingest state, mappings и pending rows

Generation
  disk-level publication набора segment references через CURRENT
```

`semantic.Snapshot` удалён из semantic API. Read-only query view представлен
`ReadView`, физический persistence payload — `SealedSegment`, а сериализация
mutable state в будущем получит отдельный `MutableStateCheckpoint`. Ни один из
этих immutable объектов не является writable: mutable остаётся
`SemanticIndex`/`Service`.

## 1. Что Сейчас Реализовано

### 1.1 Mutable semantic service

`semantic.Service` хранит:

- transitional mutable `head` source для append/capacity/compact;
- immutable HNSW `segments` component set
  (`pkg/semantic/service.go:15-28`);
- соответствие document ID к стабильным `VectorID`;
- `VectorRow` и mapping stable ID -> local ordinal;
- immutable `vector.BitSet` для liveness;
- pending vector rows until the next buffered flush.

Добавление и replace сначала добавляют vectors в pending buffer. `Flush`
строит отдельный HNSW segment вне service lock и затем публикует его. Удаление
меняет document mapping/liveness; физические строки старых components остаются
до `Compact`.

Обычный `Service.Search*` получает список immutable segment views под коротким
`RLock`, снимает lock и выполняет HNSW query по всем visible components
(`pkg/semantic/search.go:46-52`, `151-235`). Visibility публикуется после
явного `Flush`; до этого pending mutations не входят в visible component set.

### 1.2 ReadView и HNSW

`Service.ReadView`:

1. flush-ит pending mutations;
2. захватывает текущий immutable component set;
3. возвращает `ReadView` без копирования vectors;
4. оставляет compaction отдельной операцией `Service.Compact`.

HNSW создаётся при flush и compaction. `ReadView` используется для read-only
query, а `SealedSegment` — для standalone persistence.

Таким образом, текущая цепочка выглядит так:

```text
Add/Replace/Delete
        |
        v
pending ingest buffer + current published HNSW segments
        |
        | Flush()/SaveSealedSegment()/PublishSealedSegment()
        v
immutable HNSW segment / generation
```

### 1.3 Sealed semantic segment

`Segment` в текущем semantic package является HNSW-only: он содержит
authoritative source, graph, rows и `SegmentMetadata` и создаётся через
`BuildSegment`/`NewSegment`
(`pkg/semantic/segment.go:18-45`). Flat/fallback constructors удалены из
semantic runtime и низкоуровневого HNSW API. Exact flat search остаётся только
отдельным benchmark/reference oracle.

### 1.4 Persistence

`semanticpersist.PublishSealedSegment` принимает именно `SealedSegment`.
Publish записывает:

- vector matrix;
- optional graph;
- semantic state с rows и descriptors;
- manifest;
- `CURRENT` как commit pointer.

`Open` возвращает `semanticpersist.Loaded` с `SealedSegment` и generation
metadata. Reader держит shared OS lock до `Close`, поэтому активный loaded
reader блокирует публикацию новой generation.

Механизм atomic publication, checksums, hash binding и recovery в целом хорошо
проработан. Проблема не в надёжности записи, а в том, что persistence protocol
стал обязательным semantic lifecycle. Persistence теперь знает только о sealed
segment payload и generation metadata.

## 2. Сравнение С Lexical Архитектурой

### 2.1 Индексы

В lexical engine `fts.Service` принимает абстракцию `fts.Index`. Индексы
`slicedradix`, `hamt` и `index/flat` отвечают за mutable exact lookup
(`pkg/fts/multifield.go:29-81`). Они не являются persistence layer.

Для построения immutable lexical segment используется отдельный контракт
`segment.Source`. Mutable index экспортирует term/posting stream через
`ExportSegmentTerms` (`pkg/index/flat/flat.go:374-391`), а
`segment.BuildFromSourceWithTombstones` строит read-only representation.

Итого lexical разделяет:

```text
mutable index -> exported source -> sealed segment reader
```

Semantic сейчас разделяет это только частично:

```text
mutable flat vector index -> snapshot flat reader -> HNSW
```

В semantic отсутствует самостоятельный ingest/index lifecycle, который сразу
строит ANN segment. `flat` одновременно является storage head, exact search
implementation и source для будущего HNSW. Это и есть главный архитектурный
перекос.

Важно различать два утверждения:

- технически HNSW допустимо строить, читая векторы из flat source;
- архитектурно неправильно делать flat обязательным runtime search layer и
  делать HNSW производным только от snapshot.

Первое является способом построения. Второе является проблемой текущего
semantic lifecycle.

### 2.2 Tokenizer и analyzer descriptors

Lexical pipeline находится в `pkg/textproc`:

- `Tokenizer` преобразует текст в tokens (`pkg/textproc/tokenizer.go:8-10`);
- `Pipeline` применяет tokenizer и filters
  (`pkg/textproc/pipeline.go:20-78`);
- `Descriptor` содержит имя, версию и fingerprint
  (`pkg/textproc/pipeline.go:26-37`).

`ftspersist` записывает analyzer descriptor в segment manifest и проверяет его
при загрузке (`pkg/ftspersist/segment.go:92-100`, `167-173`). Это защищает
индекс от тихого использования с несовместимым tokenizer/filter pipeline.

Semantic search получает уже готовые `[]float32` и не вызывает tokenizer. Это
правильная граница: alphanumeric tokenizer, stemming и stop-word filters не
описывают embedding model и не должны участвовать в вычислении nearest
neighbors.

Но semantic сейчас хранит только:

- `SpaceDescriptor` с ID, dimensions, metric и format version;
- `ChunkingDescriptor` с одной строкой ID.

В semantic нет отдельного проверяемого descriptor-а для:

- embedding model/provider;
- model revision или embedding schema version;
- chunker version/configuration;
- preprocessing, который был выполнен до вызова внешней embedding model.

`Space.ID` и `Chunking.ID` могут использоваться как convention, но это слабее
явного compatibility contract в `textproc.Descriptor`. При замене модели,
размерности или алгоритма chunking нельзя надёжно отличить совместимый сегмент
от несовместимого.

Рекомендация: semantic не должен импортировать `textproc` ради повторного
использования lexical tokenizer. Нужен аналогичный, но отдельный
`EmbeddingDescriptor` и более строгий `ChunkingDescriptor`.

### 2.3 ReadView, MutableStateCheckpoint и segment

Lexical persistence явно различает два режима (`pkg/ftspersist/snapshot.go` и
`pkg/ftspersist/segment.go`):

| Режим | Назначение | После загрузки |
|---|---|---|
| MutableStateCheckpoint | сохранить mutable service/index и продолжить запись | writable state после восстановления |
| SealedSegment | экспортировать один compact immutable ANN-компонент для чтения | read-only |
| ReadView | зафиксировать coherent набор visible segments | read-only |

Lexical segment не является snapshot-ом. Он может быть загружен через file
reader или mmap (`pkg/ftspersist/segment.go:18-23`, `215-250`), а manifest
содержит только необходимые metadata и references.

В старой semantic API один `Snapshot` выполняет сразу четыре роли:

- coherent read view;
- compacted vector matrix;
- source для HNSW build;
- persistence payload для `semanticpersist.Publish`.

В целевой API эти роли разделяются:

- `ReadView` — coherent logical view над несколькими segments;
- `SealedSegment` — один physical ANN component;
- `MutableStateCheckpoint` — optional checkpoint mutable ingest state;
- `Generation` — disk publication references.

Именно смешение этих ролей объясняет старую зависимость от Snapshot. В новой
модели HNSW segment можно построить и открыть без `ReadView`, а обычный query
может захватить дешёвый `ReadView` без копирования vectors.

### 2.4 Tombstones и compaction

Lexical service позволяет индексировать mutable данные, а sealed segment строится
с учётом tombstones. Compaction является отдельным lifecycle и не меняет смысл
обычного search API.

Semantic использует похожие идеи stable ID и tombstone-like liveness, но только
для одного flat head:

- `ReplaceDocument` оставляет старые строки физически
  (`pkg/semantic/service.go:175-243`);
- `Compact` под write lock полностью перестраивает head
  (`pkg/semantic/service.go:130-172`);
- capacity проверяется по `s.head.Len()`, то есть по physical rows, а не по live
  rows (`pkg/semantic/service.go:178-180`).

Это делает ручной `Compact` обязательным для восстановления capacity. Для HNSW
эта модель ещё дороже: после compact или крупного batch нужно заново строить
единственный graph.

## 3. Основные Findings

### F1 — Critical (historical baseline): semantic runtime initially не использовал HNSW

`Service` хранит только `*vectorflat.Index`, а `searchViewLocked` передаёт его
как `vector.Searcher` (`pkg/semantic/service.go:15-27`,
`pkg/semantic/search.go:140-145`). HNSW появляется только после явного
`Snapshot.WithHNSW`.

Это было состояние до текущего HNSW-first increment. В текущем worktree
`Service` уже ищет по `segments`, поэтому исходное finding закрыто только в
части runtime search. Оставшаяся проблема — lifecycle и стоимость публикации:

 - mutations сначала попадают в pending ingest buffer;
 - `Flush` строит buffered batch вне service write-lock и публикует immutable
   HNSW delta segment после проверки captured mutation version;
 - vectors не дублируются в старом mutable flat head; оставшийся transitional
   storage concern относится к persistence/checkpoint lifecycle.

Исторические следствия исходной реализации были:

- обычный semantic search не имел ANN quality controls и всегда использовал
  прежний exact runtime path;
- semantic search не имеет ANN latency profile в обычном runtime;
- стоимость построения graph вынесена на caller;
- ingestion и search используют разные физические индексы;
- при каждом новом coherent snapshot HNSW нужно строить заново;
- HNSW не может быть обновлён без нового snapshot.

Это не соответствует смыслу semantic search и не решается включением fallback
policy.

### F2 — Critical (historical baseline): flat являлся смысловым fallback

В исходной реализации `SegmentKindFlat` и `hnsw.ExactFallbackSearcher` делали
exact path частью semantic public model. Сейчас flat и fallback constructor
удалены; exact scan остаётся только отдельным benchmark/reference oracle.

Exact scan не является semantic ANN strategy. Он не ищет approximate neighbors,
не даёт HNSW behavior и скрывает деградацию производительности за одним API.

Предлагается:

- удалить `SegmentKindFlat` из semantic search model;
- удалить fallback policy из `semantic.SealedSegment`, Snapshot и manifest;
- оставить `vector/flat` только как benchmark/reference exact implementation и,
  если это нужно codec-у, как generic `PreparedVectorSource`/vector file format;
- не добавлять fallback API обратно в `vector/hnsw` или semantic runtime.

### F3 — Critical: Snapshot стал обязательным operational primitive

Текущая API-последовательность для HNSW и persistence выглядит так:

```go
snapshot, err := service.Snapshot(ctx)
snapshot, err = snapshot.WithHNSW(ctx, buildOptions, fallback)
generation, err := semanticpersist.Publish(ctx, root, id, snapshot, options)
```

Это было описание старого lifecycle. В текущем worktree обычный search уже
работает по HNSW segment set, но persistence и export всё ещё используют
`Snapshot` как основной payload, а mutation lifecycle синхронно строит segments.

Snapshot должен быть опциональным immutable read/export view. Он не должен быть
единственным способом получить рабочий ANN index.

### F4 — High: HNSW Reader частично разделяет ownership vector storage и topology

`hnsw.Reader` содержит `source vector.PreparedVectorSource`
(`pkg/vector/hnsw/reader.go:11-30`). При `newReaderFromGraph` source создаётся
как новый `matrixSource` с копией `graph.values`
(`pkg/vector/hnsw/reader.go:33-45`). После текущего изменения in-memory build
может передать immutable source в reader, поэтому reader больше не обязан
создавать вторую matrix после freeze. Однако builder временно держит свою
рабочую matrix, а topology всё ещё не выражена самостоятельным public object.

Для persistence это уже исправляется частично: `OpenIndexReader` принимает внешний
vector source и связывает graph с vector file reference
(`pkg/vector/hnsw/graph_format.go:188-223`, `311-378`). In-memory
`hnsw.BuildIndexReader` сразу связывает построенную topology с переданным immutable
source. Для persistence отдельное связывание не требуется: `OpenIndexReader` сразу
возвращает reader с authoritative source.

Следствия:

- во время build возможна временная двойная память source + builder matrix;
- `SealedSegment` считает graph одновременно topology и authoritative vectors;
- `NewChunkHNSWSegment` не принимает явно vector source, потому что он спрятан
  внутри graph;
- трудно атомарно заменить topology, сохранив тот же vector source;
- disk и in-memory ownership semantics различаются.

Целевой контракт должен быть таким:

```text
VectorSource: authoritative prepared rows
HNSWTopology: node mapping + levels + adjacency + search config
HNSWSegment: immutable pair (VectorSource, HNSWTopology)
```

Topology может ссылаться на source, но не должна владеть второй matrix, если
source уже immutable и живёт достаточно долго.

### F5 — High: build/compaction всё ещё сериализуют mutations одним Service lock

Текущие `Service.Search*` уже снимают `RLock` после формирования списка
immutable segment views и выполняют ANN query вне lock
(`pkg/semantic/search.go:46-52`, `81-87`). Однако `appendVersionLocked` строит
HNSW внутри `s.mu.Lock`, `Snapshot` копирует source под `RLock`, а `Compact`
удерживает write lock во время полного rebuild
(`pkg/semantic/service.go:123-131`, `184-240`,
`pkg/semantic/snapshot.go:35-59`).

Для небольших datasets это корректно. Для production semantic workloads это
создаёт backpressure:

- долгий HNSW build блокирует writer;
- snapshot большого dataset блокирует mutations;
- compaction блокирует и readers, и writers;
- невозможно асинхронно построить HNSW поверх стабильного view без удержания
  service lock на materialization.

Целевая модель должна публиковать immutable segment одним коротким swap под
lock. Build и merge должны выполняться над frozen source вне critical section.

#### Lock contract для build и publish

Здесь важно разделять не «строительство graph вообще» и «отсутствие lock», а
два разных состояния данных:

```text
mutable service state
        | short lock
        v
freeze immutable input + capture generation
        |
        | lock released
        v
build HNSW topology/segment on frozen input
        |
        | short lock
        v
validate generation + publish/swap segment set
```

Lock обязателен в следующих местах:

- при чтении текущего ingest buffer и liveness state, если из них создаётся
  frozen input;
- при изменении document-to-vector mappings, tombstones, component set и
  generation counter;
- при публикации нового segment set, чтобы reader увидел либо старую, либо
  новую coherent версию, но не промежуточное состояние;
- при проверке, что состояние, для которого строился graph, всё ещё актуально.

Lock не обязан удерживаться во время:

- чтения vectors из уже immutable frozen source;
- HNSW graph construction;
- merge/compaction immutable segments;
- serialization и checksum calculation готового immutable segment.

Иными словами, сам graph builder должен быть защищён только от concurrent
доступа к его собственному mutable состоянию. Он не должен удерживать lock
`Service`, потому что builder работает на отдельном immutable input и не
публикуется до завершения. Если удерживать service lock на весь build, то
долгий HNSW build блокирует mutations и readers, хотя они могли бы работать с
предыдущим immutable segment set.

Если во время build произошла mutation, есть два корректных варианта:

- build публикуется только при совпадении captured generation; при mismatch
  результат отбрасывается и строится заново;
- build использует snapshot/frozen batch, а новые mutations остаются в ingest
  buffer и попадут в следующий segment.

Текущий `Flush` уже соблюдает этот контракт: `captureFlushState` снимает
immutable input под коротким `RLock`, `buildPendingSegment` работает без
`Service.mu`, а publish проверяет `mutationVersion` перед swap
(`pkg/semantic/service.go:205-252`). Оставшийся lock/build work относится к
compaction и persistence API, а не к добавлению mutable lock вокруг HNSW
builder.

### F6 — High: capacity и visibility привязаны к physical flat head

`appendVersionLocked` проверяет свободное место через
`MaxVectors - s.head.Len()` (`pkg/semantic/service.go:175-180`). Replace
document может быть отклонён, хотя достаточно live capacity; сначала требуется
`Compact`.

Для сегментной ANN модели capacity должна быть отдельной политикой:

- ingest head имеет собственный лимит;
- sealed segments имеют собственные лимиты;
- deletes не требуют немедленного физического удаления;
- merge compaction освобождает storage асинхронно;
- query видит только опубликованный set segments плюс явно определённый active
  head.

Нужно выбрать и документировать visibility contract. Варианты:

- synchronous seal: mutation видна после построения маленького HNSW segment;
- buffered visibility: mutation сначала попадает в ingest head и становится
  searchable после flush;
- dual generation: query читает предыдущий immutable set и текущий sealed
  delta set.

Рекомендуется buffered/segment visibility с явным `Flush` и маленькими delta
segments. Это честнее, чем скрытый exact fallback.

Текущий код использует buffered model: mutation пишет в pending ingest buffer,
а `Flush` freeze/builds и публикует segment. Поэтому основная visibility policy
уже выбрана; остаются capacity accounting, stale-ratio statistics и
component-local liveness optimizations.

Нужно не смешивать её с synchronous seal-моделью:

- buffered model: mutation пишет только в ingest buffer, `Flush` freeze/builds
  и публикует segment;
- synchronous seal допускается только как explicit caller policy, которая
  вызывает `Flush` после mutation, а не как скрытый runtime fallback.

Для целевой архитектуры рекомендуется buffered model. Capacity ingest buffer,
capacity sealed segments и storage budget должны быть отдельными политиками.

### F7 — High: semantic API не управляет ANN quality

`SearchDocuments` принимает только `ctx`, query и `k`
(`pkg/semantic/search.go:52-68`). `SearchOptions.EfSearch` и
`VisitLimit` доступны в `vector.SearchOptions`, но до semantic API не доходят.

Значит, caller не может на request level выбрать latency/recall trade-off.
Search config зашит в graph при build, а `SearchDocuments` дополнительно
использует `MaxChunkCandidates` как grouping budget
(`pkg/semantic/search.go:58-94`). Это смешивает:

- ANN traversal budget;
- количество candidate chunks;
- количество returned documents.

Нужно разделить public options:

- `K` — результат;
- `CandidateChunks` — budget для document grouping;
- `EfSearch` и `VisitLimit` — ANN work limits;
- optional per-segment or global deadline/cancellation;
- explicit `Incomplete` semantics.

### F8 — Medium: filtered HNSW search не имеет отдельной recall policy

После replace/delete graph содержит stale nodes, а liveness передаётся как
`ResultFilter`. HNSW может посещать rejected nodes, но возвращает только live
vectors (`pkg/vector/hnsw/search.go:70-80`, `204-242`). При высокой доле stale
nodes traversal может завершиться с низким количеством accepted candidates или
`Incomplete`, даже если live neighbors существуют.

Сейчас это скрыто за общим `GroupingIncomplete` и не различает:

- ANN traversal не достиг budget;
- большая часть посещённых nodes была rejected;
- segment физически требует rebuild;
- результат неполон из-за document grouping budget.

В целевой реализации нужны per-segment stats и порог compaction по stale
ratio. Exact scan не следует добавлять для исправления этой проблемы: решение —
rebuild/merge HNSW segment или увеличить ANN budget.

### F9 — Medium: document grouping корректен только как approximate candidate grouping

`SearchDocuments` сначала получает до `MaxChunkCandidates` chunk hits, затем
группирует их по document и берёт distance первого hit
(`pkg/semantic/search.go:78-94`, `97-137`). При ANN search или ограниченном
candidate budget это не гарантирует top-k документов по полному набору chunks.

Это допустимо, но должно быть частью явного API contract:

- `Distance` документа означает лучший найденный chunk, а не exact best chunk;
- `GroupingIncomplete` означает, что document ranking может быть неполным;
- для strict mode нужен отдельный expensive multi-stage rerank, а не flat
  fallback.

### F10 — Medium: stable ID abstraction опережает текущий one-head lifecycle

`ComponentID`, `Location.Component` и `MutableHeadID` уже существуют
(`pkg/semantic/types.go:23-32`), но сейчас все locations принадлежат одному
component (`pkg/semantic/service.go:113-120`, `230-235`). Это сигнал, что
сегментная модель предполагалась, но не доведена до публичного lifecycle.

Вместо сохранения fake `MutableHeadID` нужно сделать component/segment identity
реальной частью index manifest и query result mapping.

### F11 — Medium: descriptors не гарантируют совместимость embeddings и chunks

`Snapshot.Validate` проверяет dimensions, metric, normalization, vector format,
rows и chunk uniqueness (`pkg/semantic/snapshot.go:112-164`). Это хорошая
структурная проверка, но она не проверяет semantic provenance.

Нужно добавить, минимум:

- embedding model ID;
- model revision или immutable fingerprint;
- preprocessing fingerprint;
- chunker ID/version/fingerprint;
- vector format version;
- optional source collection/schema ID.

При открытии segment несовместимый descriptor должен давать explicit mismatch,
как analyzer fingerprint gate в `ftspersist`.

### F12 — Medium: часть тестов и документации закрепляет transitional design

Часть тестов всё ещё проверяет snapshot-driven graph build или использует
Snapshot как основной search object:

- `pkg/semantic/segment_test.go:14-38` проверяет HNSW path, но fixture строит
  его через Snapshot;
- `pkg/semantic/segment_test.go:41-95` вручную строит HNSW после snapshot;
- `pkg/semanticpersist/store_test.go:95-121` проверяет HNSW persistence round
  trip, но всё ещё делает Snapshot главным persistence input;
- `pkg/semantic/snapshot_test.go` рассматривает Snapshot как основной search
  object.

После архитектурного изменения эти тесты нужно заменить контрактными тестами:

- semantic search использует только HNSW segments;
- отсутствие fallback является проверяемым свойством;
- mutation/flush/search visibility явно тестируется;
- segment можно открыть без создания mutable service;
- snapshot persistence не нужен для обычного ANN query.

### F13 — High: segment identity и source binding не являются частью persistence

`NewHNSWSegment` проверяет длину source и graph, но не проверяет, что graph
построен именно над переданным authoritative source
(`pkg/semantic/segment.go:31-45`). В persistence каждый открытый segment также
получает `MutableHeadID` вместо сохранённого component ID
(`pkg/semanticpersist/segment.go:186-198`,
`pkg/semanticpersist/store.go:551-592`).

Нужно добавить в segment metadata:

- immutable `ComponentID`/segment object ID;
- source identity/fingerprint;
- graph identity и его build version;
- ordinal count/dimensions/metric binding.

Open должен отвергать graph/source mismatch, а generation manifest должен
содержать identity каждого component-а, а не только тип HNSW.

### F14 — High: descriptor compatibility пока не проверяется при open

Текущий `SpaceDescriptor` содержит `ModelVersion` и `Fingerprint`, а state
format v3 их сериализует, но отдельного `EmbeddingDescriptor` нет и при
`OpenSegment`/`Open` не передаётся expected descriptor
(`pkg/semantic/types.go:31-45`, `pkg/semanticpersist/state_format.go:30-40`).

Поэтому старый segment можно открыть без доказательства, что embedding model,
preprocessing и chunker совместимы с текущим consumer-ом.

Нужно:

- ввести `EmbeddingDescriptor` с обязательным immutable fingerprint;
- оставить `ChunkingDescriptor` отдельным контрактом;
- принимать expected descriptors в open API;
- возвращать отдельные ошибки embedding mismatch и chunking mismatch;
- включить descriptor identity в segment/generation manifest.

### F15 — Medium: request API не отделяет candidate budget от ANN budget

`SearchOptions` пока содержит только `EfSearch` и `VisitLimit`
(`pkg/semantic/types.go:79-84`). `SearchDocumentsWithOptions` использует
конфигурационный `MaxChunkCandidates`, поэтому caller не может задать
`CandidateChunks` на уровне запроса (`pkg/semantic/search.go:81-121`).

Нужно добавить `CandidateChunks` в public document-search options и явно
разделить:

- `k` returned documents/chunks;
- `CandidateChunks` grouping budget;
- `EfSearch` и `VisitLimit` ANN traversal budget.

### F16 — Medium: liveness filter материализуется на каждый query

`segmentViewsLocked` на каждом запросе строит глобальный `liveIDs` и новый
boolean filter для каждого segment (`pkg/semantic/search.go:125-148`). Это
делает стоимость query линейной по всему историческому объёму и создаёт
лишние аллокации.

Нужно либо хранить component-local liveness/tombstone state, либо обновлять
immutable read view при mutation/flush. Пересоздание фильтров на каждый search
не должно быть целевой реализацией.

## 4. Целевая Архитектура

### 4.1 Слои

#### Layer A: vector space and metadata

Оставить `pkg/vector.Space`, `Metric`, `Normalization` и подготовку rows. Они
уже хорошо определены и проверяют dimensions, finite values, zero norm и
canonical representation (`pkg/vector/metric.go:17-157`).

Добавить semantic descriptor:

```go
type EmbeddingDescriptor struct {
    ModelID          string
    ModelVersion     string
    Dimensions       int
    Metric           vector.Metric
    Normalization    vector.Normalization
    VectorFormat     uint32
    Fingerprint      string
}

type ChunkingDescriptor struct {
    ID          string
    Version     uint32
    Fingerprint string
}
```

`Fingerprint` должен рассчитываться из immutable compatibility identity, а не
из runtime pointer. Если caller сам генерирует embeddings, он всё равно обязан
передать descriptor, чтобы persisted data имела provenance.

#### Layer B: immutable vector source

Ввести явный source contract для prepared vectors и chunk rows:

```go
type VectorSegment struct {
    ID       ComponentID
    Vectors  vector.PreparedVectorSource
    Rows     []VectorRow
    Live     vector.ResultFilter
    Metadata SegmentMetadata
}
```

`Rows[ordinal]` остаётся mapping local ordinal -> stable `VectorID` + `chunk.Ref`.
Rows и vector source должны быть immutable и иметь одинаковый `Len`.
В текущем transitional `Snapshot` поле `Rows` дублирует segment rows; после
перехода на segment-owned metadata это поле должно быть удалено, а validation,
search и persistence должны читать rows только из segment.
Внутренний semantic search использует безопасные методы `rowCount`/`rowAt` без
выдачи mutable slice; публичный accessor должен возвращать defensive copy.

Source не обязан быть `flat.Reader`. Это может быть:

- in-memory contiguous prepared matrix;
- mmap/file-backed vector reader;
- compact vector block;
- test source.

`flat.Reader` может реализовывать source contract, но semantic API не должен
требовать именно его type.

#### Layer C: HNSW topology

В `pkg/vector/hnsw` разделить topology и source ownership.

Логический API:

```text
BuildIndexReader(ctx, source, options) -> HNSW searcher
WriteGraph(reader, vectorFileReference)
OpenIndexReader(graph, source, vectorFileReference)
```

Внутренний builder может временно хранить подготовленные values для
construction, но после freeze reader не должен копировать matrix, если source
уже immutable. Текущий `OpenIndexReader` уже демонстрирует правильную persistence
границу; её нужно сделать основной и для in-memory build.

HNSW graph должен содержать:

- node-to-vector ordinal mapping;
- levels and adjacency;
- entry point;
- build and search config;
- graph format/build version;
- vector source identity.

Graph не должен содержать semantic chunk metadata. Это ответственность semantic
segment.

#### Layer D: immutable semantic segment

Semantic segment должен быть ANN-only:

```text
SemanticSegment
  metadata descriptors
  vector source
  HNSW reader/topology
  VectorRows
  liveness/tombstone filter
```

`Search` сегмента всегда вызывает HNSW. Никакого `SegmentKindFlat` и
`ExactFallbackPolicy` в semantic package.

Для пустого или очень маленького segment всё равно используется HNSW contract.
Если алгоритм хочет оптимизировать trivial one-row case внутри vector package,
это должно оставаться внутренней implementation optimization и не менять
semantic strategy в exact fallback.

#### Layer E: mutable index and segment set

Вместо одного `head` нужен immutable segment set плюс ingest head:

```text
SemanticIndex
  mu
  active ingest buffer
  visible immutable HNSW segments
  document -> current vector IDs
  vector ID -> (segment, ordinal)
  tombstones/deletions
  generation/version
```

Рекомендуемый lifecycle:

1. `AddDocument` validates and appends prepared vectors to ingest buffer.
2. `ReplaceDocument` appends a new version and marks old IDs stale.
3. `DeleteDocument` marks current IDs stale.
4. `Flush` freezes a batch and builds one HNSW segment вне service lock.
5. Short publish lock adds the new segment and updates visibility metadata.
6. Query searches all visible HNSW segments and merges hits.
7. Background merge rebuilds a compact HNSW segment from live rows.
8. Short publish lock swaps old components for the merged component.

Если immediate visibility обязательна, `AddDocument` может синхронно выполнять
flush для маленького batch. Но это должно быть explicit policy, а не случайный
эффект `Snapshot`.

### 4.2 Search aggregation

Внутренний chunk-candidate stage:

1. определить visible segment set под коротким read lock;
2. отпустить lock;
3. выполнить HNSW query на каждом segment;
4. применить segment-local liveness filter;
5. объединить hits по distance и local tie-break order;
6. преобразовать ordinal в `ChunkHit` через соответствующий `Rows`;
7. суммировать per-segment stats;
8. передать агрегированный `Incomplete` в document grouping.

Для document search:

1. запросить `CandidateChunks` с каждого segment;
2. глобально отсортировать chunk candidates;
3. сгруппировать по `DocID`;
4. ограничить chunks per document;
5. вернуть `GroupingIncomplete`, если candidate или ANN budget не покрывает
   visible set.

Нельзя использовать один global `ResultFilter`, потому что ordinals локальны
для segment. Filter должен быть component-local.

Публичный API-слой:

```go
type SearchOptions struct {
    EfSearch         int
    VisitLimit       int
    CandidateChunks  int
}

func (s *Service) SearchDocumentsWithOptions(
    ctx context.Context,
    encoder Encoder,
    query Document,
    k int,
    options SearchOptions,
) (DocumentSearchResult, error)
```

`Encoder` выполняет только преобразование `Document -> ChunkVector`; orchestration
add/replace/search остаётся в `semantic.Service`. Query должен проходить через
тот же encoder/model, что и indexed documents. Для query с несколькими chunks
результаты отдельных embeddings объединяются по `DocID`, score документа равен
минимальной найденной дистанции.

Chunk candidates остаются внутренним этапом и наружу не публикуются. Публичный
результат всегда сгруппирован по `DocID`; найденные chunks доступны внутри
`DocumentHit.Chunks` как объяснение результата.

### 4.3 ReadView как логическое view, а не обязательный artifact

Нужно разделить понятия:

```text
ReadView
  coherent immutable view of currently visible components

SealedSegment
  one immutable searchable ANN component

MutableStateCheckpoint
  optional serialization of mutable ingest state and metadata

Generation
  optional disk publication pointer over persisted components
```

`ReadView` может быть дешёвым: список immutable segments, rows metadata и
generation number. Он не обязан копировать все vectors. Создание нового dense
component нужно только когда caller действительно хочет выполнить merge или
export.

Обычный ANN search должен работать непосредственно через `SemanticIndex` и
visible segment set.

### 4.4 Persistence API

Сохранить идеи `semanticpersist`, но изменить объект входа и выход:

```text
SaveSealedSegment / OpenSealedSegment
  writes/opens one immutable HNSW segment

SaveStateCheckpoint / LoadStateCheckpoint
  optional mutable ingest state, document mappings and pending rows

PublishGeneration / OpenGeneration
  optional CURRENT + lock + atomic publication over segment references
```

`OpenSealedSegment` должен возвращать read-only `SealedSegment`, а не mutable
`Service`. `OpenGeneration` должен возвращать
read-only `ReadView` над набором opened segments. `LoadStateCheckpoint` может
восстановить mutable state, но это отдельная операция и отдельный тип.

Generation manifest должен содержать:

- segment object ID;
- vector file reference;
- graph file reference;
- semantic descriptors;
- rows metadata reference;
- liveness/version metadata;
- format/build versions.

Текущий hash-chain, CRC, atomic rename, `CURRENT`, `ExpectedGeneration`,
`ErrIndeterminate` и explicit `RepairCurrent` можно сохранить. Это зрелая часть
реализации (`pkg/semanticpersist/store.go:253-309`).

Не следует сохранять fallback policy и `SegmentKindFlat` в новом manifest.

Lock policy нужно выбрать отдельно:

- disk `OpenGeneration` может держать shared lock для file-backed readers;
- in-memory `OpenSegment` не должен блокировать mutable index;
- publish должен требовать exclusive lock только на filesystem publication;
- retention/GC должен быть отдельным API.

### 4.5 Real-time updates: mutable HNSW versus delta segments

Внешние реализации не дают оснований делать текущий `Searcher` mutable:

- `hnswlib` предоставляет add-point и поддерживает удаление через отдельную
  deleted-labels модель, но shared mutable graph требует собственной
  синхронизации вокруг concurrent add/search/save;
- Faiss разделяет индексы по update semantics: некоторые структуры допускают
  `add`, но graph-based варианты не превращаются от этого в безопасный
  lock-free concurrent writer/reader index; composite `IndexShards` и
  `IndexReplicas` решают масштабирование, а не atomic document visibility;
- Qdrant и Milvus используют сегменты, sealed/ growing state и background
  compaction, а не mutation уже опубликованной immutable topology.

Практический вывод для этого repository: real-time add не означает, что один
HNSW graph должен быть изменяемым во время поиска. Нужен near-real-time
contract: mutation попадает в bounded pending buffer, `Flush` строит новый
immutable HNSW segment, а query видит его после atomic publication. Малые
delta segments дают более короткую задержку видимости; compaction объединяет
их позже.

True mutable HNSW можно добавить только как отдельную implementation policy,
если измерения покажут, что flush latency неприемлема. Это потребует явных
гарантий для concurrent add/search, delete visibility, rollback/cancellation,
graph growth и persistence consistency. Добавлять такую сложность в текущий
API без требования по latency не следует.

Источники для этого решения:

- [hnswlib README](https://github.com/nmslib/hnswlib/blob/master/README.md)
- [Faiss index documentation](https://github.com/facebookresearch/faiss/wiki/Faiss-indexes)
- [Qdrant storage concepts](https://qdrant.tech/documentation/concepts/storage/)
- [Milvus architecture overview](https://milvus.io/docs/architecture_overview.md)

## 5. План Рефакторинга

### Phase 0 — Freeze contract и migration decision

Цель: перестать расширять старый lifecycle.

Статус: реализовано как contract freeze и migration decision. Старые persisted
formats не reinterpret-ируются автоматически как ANN; для них требуется
read-only compatibility или explicit graph rebuild.

#### Decision Record

Phase 0 фиксирует следующие решения для дальнейшей реализации:

- breaking change внутри semantic package допустим, но migration boundary
  должен быть виден в API и документации;
- `flat` exact search не является semantic search strategy;
- exact fallback policy удалена; exact flat search используется только как
  отдельный benchmark/reference oracle;
- старые snapshot-driven и flat-generation APIs не являются compatibility
  surface semantic runtime; старые данные требуют явного rebuild;
- целевая visibility policy — explicit buffered flush с immutable HNSW delta
  segments; automatic exact visibility через flat не вводится;
- до появления buffered flush текущий `Service` считается transitional
  implementation, а не целевой HNSW runtime;
- минимальный compatibility descriptor должен отдельно описывать embedding
  space/model и chunking contract;
- старые flat generations не интерпретируются автоматически как ANN
  generations; для них нужен explicit graph rebuild/migration;
- обычный in-memory semantic search не должен требовать snapshot или disk
  persistence.

Это решение было начальным migration boundary. Реализация продолжена через
следующие фазы в той же ветке: search implementation теперь HNSW-first,
а buffered `Flush` и atomic segment publication уже реализованы в Phase 3.

Результат:

- semantic runtime использует HNSW-only segments;
- exact fallback не входит в semantic API;
- visibility policy зарезервирована за будущим buffered flush; текущий
  synchronous seal не маскируется под этот API;
- минимальный embedding-space/chunking descriptor contract принят и входит в
  state format; расширенный provenance compatibility gate остаётся Phase 6;
- breaking change принят, так как feature не находится в production.

Acceptance criteria:

- в design/API тестах нет требования к flat fallback;
- документирована семантика visibility и `Incomplete`;
- migration policy для старых persisted generations принята: старые formats
  только read-only compatibility или explicit graph rebuild, автоматического
  reinterpretation как ANN нет.

### Phase 1 — Разделить HNSW source и topology

Цель: устранить дублирование vectors и сделать HNSW самостоятельным индексом.

Статус: реализовано для in-memory и graph persistence API. Публичный in-memory
путь теперь представлен одним `hnsw.BuildIndexReader`; topology остаётся приватной
частью reader. Для persistence используется `OpenIndexReader`, который сразу связывает
graph с authoritative vector source.

Изменения:

- выделить immutable topology object или internal constructor;
- изменить `hnsw.BuildIndexReader`, чтобы после freeze reader ссылался на переданный
  immutable source вместо копирования matrix;
- сохранить проверку source dimensions/metric/normalization;
- оставить graph file binding к vector file reference;
- добавить ownership/lifetime tests;
- проверить concurrent search при закрытии/замене source.

Acceptance criteria:

- HNSW build из любого `PreparedVectorSource`;
- in-memory build не создаёт второй обязательный vector matrix;
- graph reader и vector source имеют отдельные тесты и явный lifecycle;
- graph persistence round trip проходит без semantic Snapshot.

Уже выполнено:

- topology не содержит vector values после freeze;
- `BuildIndexReader` проверяет source dimensions/metric/normalization/ordinal count;
- semantic runtime использует reader, связанный с immutable source;
- добавлены тесты source metadata, graph persistence round trip и concurrent search.

Source identity на disk уже обеспечивается `VectorFileReference` (size + SHA-256).
Отдельный in-memory source fingerprint остаётся задачей semantic segment
metadata из F13, а не blocker-ом для topology/source split.

Риск: текущие тесты могут напрямую предполагать, что `hnsw.Reader` сам
является `PreparedVectorSource`. На переходе можно временно сохранить эту
реализацию, но semantic segment должен использовать явную пару source+graph.

### Phase 2 — Создать ANN-only semantic segment

Цель: убрать flat и fallback из semantic model.

Статус: реализовано. `semantic.Segment` содержит source/graph/rows и
`SegmentMetadata`, строится через `BuildSegment` и не предоставляет flat или
exact fallback constructor.

Изменения:

- заменить `SealedSegment` на HNSW-only `Segment`;
- перенести vector source, topology reader и rows в отдельные поля;
- удалить `SegmentKindFlat`;
- удалить `ExactFallbackPolicy` из semantic API и manifest;
- добавить `SegmentMetadata` с embedding/chunk descriptors;
- добавить `OpenSegment`/`BuildSegment` API;
- сделать `Validate` проверкой source, graph, rows и descriptors.

Acceptance criteria:

- semantic segment search никогда не вызывает exact scan;
- поиск segment работает без общего snapshot object в runtime path;
- vector/hnsw low-level tests остаются независимыми от semantic;
- отсутствие fallback проверяется тестом по stats/API.

### Phase 3 — Ввести segment set и flush lifecycle

Цель: сделать HNSW основным runtime index.

Статус: в основном реализовано. Mutation складывается в pending ingest buffer,
service хранит component set, replace/delete используют stable IDs, а
`Flush` строит immutable HNSW delta segment вне service lock и публикует его
после проверки mutation version. `Compact` выполняет merge. Остались
component-local stale-ratio statistics, отдельный storage/checkpoint lifecycle
и оптимизация metadata, но buffered segment lifecycle уже является рабочим
runtime контрактом.

Изменения:

- заменить single `head` на ingest buffer + immutable segment set;
- реализовать `Flush(ctx)` с freeze/build вне write lock;
- добавить generation/component ID для каждого sealed segment;
- сохранить stable `VectorID` независимо от local ordinal;
- сделать replace/delete через new version + tombstone;
- реализовать atomic component-set swap;
- добавить stale ratio и merge trigger statistics.

Дополнительно обязательно:

- отделить ingest-buffer capacity от sealed-segment/storage capacity;
- не дублировать vectors в старом flat head и immutable segment без явной
  необходимости;
- публиковать результат только после проверки captured generation;
- хранить component-local liveness/read-view metadata вместо построения полного
  filter на каждый запрос.

Acceptance criteria:

- после flush обычный `Service.Search*` использует HNSW;
- новые segment-ы видимы без Snapshot и без persistence;
- writer блокируется только на коротком commit swap;
- failed/canceled build не меняет visible segment set;
- old component remains searchable until new component is published.

В текущем worktree выполнены все acceptance criteria для buffered flush,
включая visibility barrier до `Flush`, отмену без публикации и сохранение
старого component set при устаревшем build. Открытыми для следующего шага
остаются stale-ratio trigger и persistence identity.

### Phase 4 — Реализовать multi-segment search и document grouping

Цель: перенести текущую grouping логику на component-local ordinals и ANN
options.

Статус: реализовано. Query агрегирует HNSW results всех visible components,
использует component-local filters, deterministic ordering и отдельные
`SearchDocumentsWithOptions` управляет ANN budgets. Request-level
`CandidateChunks` отделён от public `MaxK`.

Изменения:

- выполнить query по всем visible HNSW segments;
- merge chunk hits с deterministic ordering;
- суммировать stats и передавать агрегированный `Incomplete` state;
- добавить ANN `EfSearch`/`VisitLimit` options;
- отделить `CandidateChunks` от `MaxK`;
- добавить optional second-stage reranking только как отдельную функцию, если
  позже понадобится strict document ranking.

`CandidateChunks` является request-level option и ограничивается configured
maximum. Document ranking использует минимальную дистанцию chunk-а как score
документа; `GroupingIncomplete` сообщает, что candidate budget или ANN visit
limit не позволили получить полный результат.

Acceptance criteria:

- differential tests сравнивают ANN result с exact reference только как
  benchmark/quality oracle, не как runtime fallback;
- tests проверяют merge hits из нескольких segments;
- tests проверяют stale filter и `Incomplete`;
- tests проверяют deterministic tie ordering;
- tests проверяют grouping incomplete при ограниченном candidate budget.

Дополнительно покрыто тестами: merge результатов из нескольких segments,
публикация stale filters и propagation ANN `VisitLimit` в document grouping.

### Phase 5 — Разделить persistence snapshot и sealed segment

Цель: сделать disk persistence опциональной и симметричной lexical architecture.

Статус: частично реализовано. HNSW-only artifact поддерживает standalone
`SaveSegment`/`OpenSegment` без mutable service и `CURRENT`; segment-oriented
`SaveSealedSegment`/`OpenSealedSegment` и `PublishSealedSegment` добавлены как
отдельный contract; Snapshot functions удалены из semantic и persistence API.
Generation `Publish`/`Open` остаются optional publication layer. Manifest/state
не содержат fallback policy, формат manifest получил новую версию, а Open
связывает graph с vector source. Остались multi-segment generation references,
component identity в manifest и mutable checkpoint API.

Изменения:

- добавить `SaveSealedSegment`/`OpenSealedSegment` для HNSW segment;
- добавить optional `SaveStateCheckpoint`/`LoadStateCheckpoint` для mutable ingest state;
- переделать `semanticpersist.PublishSealedSegment` на references к sealed
  segments, а не на общий snapshot payload;
- сохранить content-addressed segment objects и atomic `CURRENT`;
- сохранять descriptors и stable rows отдельно от vector/graph binary;
- добавить retention/GC API для orphan segments и old generations;
- сохранять component/segment identity и source fingerprint в manifest;
- разделить `OpenSealedSegment`/`OpenGeneration` result: первый возвращает
  segment, второй — `ReadView`;
- добавить expected descriptor compatibility checks;
- описать, что `LoadedGeneration.Close` освобождает file-backed resources и
  shared lock, но не управляет in-memory service.

Acceptance criteria:

- ANN search можно запустить только из in-memory segment без store;
- persisted segment можно открыть без создания mutable service;
- mutable checkpoint можно не создавать для read-only ANN search;
- generation publication остаётся атомарной;
- повреждённые vectors, graph, rows и descriptor mismatch отвергаются;
- old format policy определена: migrate, read-only compatibility или explicit
  rebuild.

### Phase 6 — Tokenizer/chunking/model compatibility

Цель: сделать semantic provenance проверяемым и не смешивать lexical analysis с
embedding search.

Статус: частично реализовано. Базовые поля descriptors реализованы в
`SpaceDescriptor` и `ChunkingDescriptor`, сохраняются в state format v3 вместе
с fingerprint/version полями. Отдельного `EmbeddingDescriptor`, expected
descriptor при open и обязательной provenance validation пока нет. Semantic
по-прежнему принимает готовые embeddings и не вызывает lexical tokenizer.

Изменения:

- добавить `EmbeddingDescriptor` fingerprint;
- расширить `ChunkingDescriptor` version/fingerprint;
- включать descriptors в segment metadata и generation state;
- отвергать open при mismatch;
- явно документировать, что `textproc.Pipeline` относится к lexical indexes;
- если появится text-to-embedding adapter, вынести его в отдельный package,
  который принимает chunker + embedding provider и передаёт semantic готовые
  `ChunkVector`.

Acceptance criteria:

- изменение embedding model/version не может тихо открыть старый segment;
- изменение chunker не может тихо переиспользовать старые rows;
- lexical analyzer mismatch и semantic descriptor mismatch имеют разные errors;
- semantic package не вызывает alphanumeric tokenizer автоматически.

### Phase 7 — Benchmarks, documentation and removal

Цель: удалить старую модель после подтверждения новой.

Статус: частично реализовано. Runtime уже HNSW-first, а vector exact/fallback
реализации оставлены как low-level benchmark/reference oracle. Документация и
часть старых тестов всё ещё описывают transitional Snapshot-driven lifecycle.
Следующий increment должен добавить отдельные merge/stale-ratio/memory
benchmarks и обновить semantic examples.

Изменения:

- оставить flat exact benchmark как recall/latency oracle;
- добавить HNSW segment build/search/merge benchmarks;
- добавить datasets с replacements/deletes и stale ratios;
- добавить memory benchmark до/после разделения source/topology;
- обновить semantic examples на document-level `semanticencode` flow;
- обновить `readme.md` и `docs/semantic-persistence-guide.md`;
- удалить semantic flat example и fallback tests после migration window;
- удалить compatibility code, если нет внешнего persisted data requirement.

Acceptance criteria:

- benchmark names явно говорят `exact reference` или `HNSW ANN`;
- documentation не называет flat semantic search implementation;
- public API не требует Snapshot для обычного search;
- lock/build/publish benchmark показывает, что build не удерживает service lock;
- все tests и depcheck проходят.

## 6. Что Сохранить Из Текущей Реализации

Не всё в текущей ветке нужно выбрасывать.

Сохранить и переиспользовать:

- `vector.Space` и строгую подготовку vectors;
- `MetricCosine`/`MetricL2Squared` semantics;
- `PreparedVectorSource` contract;
- HNSW deterministic build, graph validation и build metadata;
- graph/vector SHA-256 binding;
- binary format magic/version/CRC checks;
- semantic stable `VectorID` и `VectorRow` idea;
- chunk uniqueness и document grouping rules как верхнеуровневую логику;
- `CURRENT`, ExpectedGeneration, atomic replace и `ErrIndeterminate`;
- explicit Repair вместо автоматического выбора orphan generation;
- context cancellation и allocation limits;
- exact flat tests как quality oracle.

Удалить или вынести из semantic runtime:

- flat mutable search head;
- `SegmentKindFlat`;
- semantic `ExactFallbackPolicy`;
- `Snapshot.WithHNSW` как основной способ запуска ANN;
- обязательную передачу Snapshot в persistence;
- assumption, что graph reader одновременно является vector storage;
- component abstraction, ограниченную одним `MutableHeadID`.

## 7. Migration And Compatibility

### Existing in-memory callers

Код, использующий `semantic.Service.AddDocument` и `Search*`, теперь сначала
попадает в pending buffer. До `Flush` новые и изменённые документы не входят в
published search view; после `Flush` результаты публикуются как HNSW delta
segment. Для immediate visibility caller может явно вызвать `Flush` после
mutation.

Ранее рассматривались варианты:

- synchronous flush для сохранения immediate visibility;
- buffered visibility и вызов `Flush` caller-ом;
- adapter, который автоматически flush-ит каждый batch в маленький HNSW
  segment.

Рекомендуется не обещать exact immediate semantics в новом API. Если backward
compatibility нужна, старый exact service можно оставить отдельным package или
compatibility mode, но не называть его semantic ANN runtime.

### Existing persisted generations

Текущий manifest содержит `SegmentKind`, optional graph и fallback policy. Есть
три безопасных варианта:

- read-only open старого format и rebuild в новый format;
- explicit migration command, который открывает old generation и строит new
  HNSW segment;
- поддержка старого format только в отдельном compatibility reader.

Автоматически интерпретировать старый flat generation как новый ANN segment
нельзя: это меняет search semantics. Нужен явный rebuild graph.

### Public API strategy

Breaking change принят. Текущий public surface содержит настоящий buffered
`Flush`; следующий public migration surface должен включать
`Search*WithOptions`, `ReadView`, `NewHNSWSegment`, `SaveSegment`,
`OpenSegment`, `SaveStateCheckpoint` и `LoadStateCheckpoint`; старые
flat/fallback/Snapshot-driven semantic paths удалены вместо сохранения
production compatibility layer.

`Flush` является visibility barrier только если он freeze-ит pending state,
строит segment вне service lock и атомарно публикует его. Это уже проверяется
текущим implementation и contract tests.

## 8. Open Questions Before Implementation

Перед кодированием нужно принять только продуктовые решения, которые нельзя
надёжно вывести из текущего кода:

- нужны ли дополнительные guarantees для visibility до `Flush`;
- достаточно ли текущего небольшого задержанного visibility window;
- нужен ли disk-backed mmap segment в первой версии;
- нужен ли multi-segment index сразу или достаточно одного sealed segment с
  rebuild;
- требуется ли strict document top-k или достаточно approximate candidate
  grouping;
- нужно ли сохранять старые semantic generations read-only;
- должен ли `text-to-embedding` orchestration жить в repository или embeddings
  всегда остаются caller responsibility.

Рекомендованные ответы для первой реализации:

- visibility после `Flush`;
- HNSW-only immutable delta segments;
- multi-segment query aggregation;
- disk segment optional, generation publication optional;
- approximate document grouping с явным `Incomplete`;
- старый format только через explicit migration/rebuild;
- embedding model и chunking выполняются caller-ом, semantic хранит их
  descriptors.

## 9. Contract Tests Before Final Removal

Перед удалением transitional lifecycle добавить контрактные тесты:

- mutation до `Flush` не видна, если выбран buffered visibility contract;
- `Flush` публикует ровно одну coherent generation;
- canceled/failed build не меняет visible component set;
- mutation и search могут работать с предыдущим segment set во время build;
- publish отклоняет результат с устаревшим captured generation;
- graph/source mismatch отвергается;
- descriptor mismatch отвергается при `OpenSealedSegment` и `OpenGeneration`;
- component identity сохраняется после persistence round trip;
- `CandidateChunks` задаётся на request level;
- liveness и stale ratio дают per-segment statistics;
- persisted multi-segment generation открывается без mutable service;
- old flat generation требует explicit rebuild/migration.

## 10. Final Decision

Текущая реализация является рабочим HNSW-first segment prototype, но ещё
не является завершённой persistence-oriented semantic ANN архитектурой,
потому что:

- component identity и source binding не являются полным persistence contract;
- descriptor compatibility не проверяется при open;
- document candidate budget не является request-level option.

Правильная модель — HNSW-first segment architecture с отдельными mutable
ingest, immutable ANN segments, multi-segment query aggregation и независимыми
ReadView/SealedSegment/Generation persistence layers. Это сохраняет сильные
части текущих vector и persistence primitives и убирает зависимость semantic
search от flat и удержания service lock во время build.
