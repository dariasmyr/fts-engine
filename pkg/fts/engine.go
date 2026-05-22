package fts

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

type docAccum struct {
	UniqueMatches int
	TotalMatches  int
	Score         float64
}

type tokenGroup struct {
	expansions []termExpansion
	totalDocs  int
	single     bool
}

type Service struct {
	indexFactory IndexFactory
	keyGen       KeyGenerator
	pipeline     Pipeline
	filter       Filter
	scorer       Scorer
	collection   *collectionStats
	registry     *DocRegistry
	tombstones   *Tombstones
	pendingRegistrySnapshot        []DocID
	pendingTombstonesSnapshot      []uint64
	pendingCollectionStatsSnapshot *CollectionStatsSnapshot
	singleField  bool

	mu      sync.RWMutex
	indexes map[string]Index
}

func New(index Index, keyGen KeyGenerator, opts ...Option) *Service {
	s := newService(keyGen, opts...)
	s.singleField = true
	s.indexFactory = func(name string) (Index, error) {
		return nil, fmt.Errorf("fts: field %q is not available (service was built with fts.New; use fts.NewMultiField to index arbitrary fields)", name)
	}
	if index != nil {
		s.indexes[DefaultField] = index
	}
	return s
}

func NewMultiField(factory IndexFactory, keyGen KeyGenerator, opts ...Option) *Service {
	s := newService(keyGen, opts...)
	s.indexFactory = factory
	return s
}

func NewMultiFieldFromIndexes(indexes map[string]Index, keyGen KeyGenerator, opts ...Option) *Service {
	s := newService(keyGen, opts...)
	for name, idx := range indexes {
		s.indexes[name] = idx
	}
	s.indexFactory = func(name string) (Index, error) {
		return nil, fmt.Errorf("fts: field %q was not present in the restored snapshot", name)
	}
	return s
}

func newService(keyGen KeyGenerator, opts ...Option) *Service {
		s := &Service{
		keyGen:     keyGen,
		pipeline:   defaultPipeline{},
		indexes:    make(map[string]Index),
		collection: newCollectionStats(),
		registry:   NewDocRegistry(),
		tombstones: NewTombstones(),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}

	if s.keyGen == nil {
		s.keyGen = WordKeys
	}

	s.finalizeRestoreState()

	return s
}

func NewFromReader(r io.Reader, loader IndexLoader, keyGen KeyGenerator, opts ...Option) (*Service, error) {
	if loader == nil {
		return nil, fmt.Errorf("fts: nil index loader")
	}

	index, err := loader(r)
	if err != nil {
		return nil, fmt.Errorf("fts: load index: %w", err)
	}

	return New(index, keyGen, opts...), nil
}

func (s *Service) IndexDocument(ctx context.Context, docID DocID, content string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if docID == "" {
		return fmt.Errorf("fts: document id is empty")
	}
	return s.indexField(ctx, docID, DefaultField, Field{Value: content})
}

func (s *Service) Delete(docID DocID) bool {
	if s == nil || docID == "" {
		return false
	}
	ord, ok := s.registry.Has(docID)
	if !ok {
		return false
	}
	if s.tombstones != nil {
		s.tombstones.Set(ord)
	}
	if s.collection != nil {
		s.collection.remove(ord)
	}
	s.registry.Forget(docID)
	return true
}

func (s *Service) UpdateDocument(ctx context.Context, docID DocID, content string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if docID == "" {
		return fmt.Errorf("fts: document id is empty")
	}
	s.Delete(docID)
	return s.IndexDocument(ctx, docID, content)
}

func (s *Service) Update(ctx context.Context, doc Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if doc.ID == "" {
		return fmt.Errorf("fts: document id is empty")
	}
	s.Delete(doc.ID)
	return s.Index(ctx, doc)
}

func (s *Service) SearchDocuments(ctx context.Context, query string, maxResults int) (*SearchResult, error) {
	return s.searchQueryString(ctx, query, "", queryFieldScope{}, maxResults)
}

func (s *Service) SearchField(ctx context.Context, field string, query string, maxResults int) (*SearchResult, error) {
	return s.searchQueryString(ctx, query, field, queryFieldScope{}, maxResults)
}

func (s *Service) SearchFields(ctx context.Context, fields []string, query string, maxResults int) (*SearchResult, error) {
	return s.searchQueryString(ctx, query, "", newQueryFieldScope(fields), maxResults)
}

func (s *Service) searchQueryString(ctx context.Context, query string, defaultField string, scope queryFieldScope, maxResults int) (*SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, exec := ensureDiagnosticsContext(ctx)

	start := exec.startTimer()

	preStart := exec.startTimer()
	parsed, err := ParseQuery(query)
	if err != nil {
		return nil, err
	}
	parsed = bindDefaultField(parsed, defaultField)
	exec.observePreprocess(preStart)

	res, err := s.searchResultForQuery(ctx, parsed, maxResults, scope)
	if err != nil {
		return nil, err
	}
	exec.observeTotal(start)
	return attachDiagnostics(ctx, res), nil
}

func (s *Service) SearchPhrase(ctx context.Context, phrase string, maxResults int) (*SearchResult, error) {
	return s.searchPhraseFieldsResult(ctx, s.fieldNames(), phrase, maxResults)
}

func (s *Service) SearchPhraseField(ctx context.Context, field string, phrase string, maxResults int) (*SearchResult, error) {
	return s.SearchPhraseFields(ctx, []string{field}, phrase, maxResults)
}

func (s *Service) SearchPhraseFields(ctx context.Context, fields []string, phrase string, maxResults int) (*SearchResult, error) {
	return s.searchPhraseFieldsResult(ctx, fields, phrase, maxResults)
}

func (s *Service) SearchPhraseNear(ctx context.Context, phrase string, distance int, maxResults int) (*SearchResult, error) {
	return s.searchPhraseNearFieldsResult(ctx, s.fieldNames(), phrase, distance, maxResults)
}

func (s *Service) SearchPhraseNearField(ctx context.Context, field string, phrase string, distance int, maxResults int) (*SearchResult, error) {
	return s.SearchPhraseNearFields(ctx, []string{field}, phrase, distance, maxResults)
}

func (s *Service) SearchPhraseNearFields(ctx context.Context, fields []string, phrase string, distance int, maxResults int) (*SearchResult, error) {
	return s.searchPhraseNearFieldsResult(ctx, fields, phrase, distance, maxResults)
}

func (s *Service) Analyze() (Stats, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var combined Stats
	found := false
	for _, idx := range s.indexes {
		analyzer, ok := idx.(Analyzer)
		if !ok {
			continue
		}
		found = true
		combined = mergeStats(combined, analyzer.Analyze())
	}
	return combined, found
}

func (s *Service) SnapshotComponents() (Index, Filter) {
	if s == nil {
		return nil, nil
	}

	s.mu.RLock()
	idx := s.indexes[DefaultField]
	s.mu.RUnlock()
	return idx, s.filter
}

func (s *Service) SnapshotCollectionStats() *CollectionStatsSnapshot {
	if s == nil || s.collection == nil {
		return nil
	}
	return s.collection.snapshot(s.registry)
}

func (s *Service) SnapshotRegistry() []DocID {
	if s == nil || s.registry == nil {
		return nil
	}
	return s.registry.Snapshot()
}

func (s *Service) SnapshotTombstones() []uint64 {
	if s == nil || s.tombstones == nil {
		return nil
	}
	return s.tombstones.Snapshot()
}

func (s *Service) BuildFilter() error {
	if s == nil || s.filter == nil {
		return nil
	}

	buildable, ok := s.filter.(BuildableFilter)
	if !ok {
		return nil
	}

	if err := buildable.Build(); err != nil {
		return fmt.Errorf("fts: build filter: %w", err)
	}

	return nil
}

func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dus", d.Microseconds())
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}

func (s *Service) finalizeRestoreState() {
	if s == nil {
		return
	}
	if s.pendingRegistrySnapshot != nil || s.pendingTombstonesSnapshot != nil {
		tombs := RestoreTombstones(s.pendingTombstonesSnapshot)
		if s.pendingRegistrySnapshot != nil {
			s.registry = RestoreDocRegistryActive(s.pendingRegistrySnapshot, tombs)
		}
		s.tombstones = tombs
		s.pendingRegistrySnapshot = nil
		s.pendingTombstonesSnapshot = nil
	}
	if s.pendingCollectionStatsSnapshot != nil {
		s.collection = newCollectionStatsFromSnapshot(s.pendingCollectionStatsSnapshot, s.registry)
		s.pendingCollectionStatsSnapshot = nil
	}
}
