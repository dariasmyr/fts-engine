// Package fts provides the full-text indexing and search service.
package fts

import (
	"context"
	"fmt"
	"sync"
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
	indexFactory                   IndexFactory
	keyGen                         KeyGenerator
	pipeline                       Pipeline
	filter                         Filter
	scorer                         Scorer
	compactionLoadFactor           float64
	autoCompactionCheck            bool
	compactionCallback             func(CompactionStats)
	collection                     *collectionStats
	registry                       *DocRegistry
	tombstones                     *Tombstones
	pendingRegistrySnapshot        []DocID
	pendingTombstonesSnapshot      []uint64
	pendingCollectionStatsSnapshot *CollectionStatsSnapshot
	singleField                    bool
	fieldPipelines                 map[string]Pipeline
	fieldPipelineDescriptors       map[string]PipelineDescriptor
	keyGenerator                   *KeyGeneratorDescriptor

	mu      sync.RWMutex
	indexes map[string]Index
}

type CompactionStats struct {
	TotalAssignedDocs   int
	LiveDocs            int
	TombstonedDocs      int
	TombstoneLoadFactor float64
}

func New(index Index, keyGen KeyGenerator, opts ...*Option) *Service {
	s := newService(keyGen, opts...)
	s.singleField = true
	s.indexFactory = func(name string) (MutableIndex, error) {
		return nil, fmt.Errorf("fts: field %q is not available (service was built with fts.New; use fts.NewMultiField to index arbitrary fields)", name)
	}
	if index != nil {
		s.indexes[DefaultField] = index
		s.setInitialFieldPipelineDescriptor(DefaultField)
	}
	return s
}

func NewMultiField(factory IndexFactory, keyGen KeyGenerator, opts ...*Option) *Service {
	s := newService(keyGen, opts...)
	s.indexFactory = factory
	return s
}

func NewMultiFieldFromIndexes(indexes map[string]Index, keyGen KeyGenerator, opts ...*Option) *Service {
	s := newService(keyGen, opts...)
	for name, idx := range indexes {
		s.indexes[name] = idx
		s.setInitialFieldPipelineDescriptor(name)
	}
	s.indexFactory = func(name string) (MutableIndex, error) {
		return nil, fmt.Errorf("fts: field %q was not present in the restored snapshot", name)
	}
	return s
}

// IsMultiField reports whether the service was constructed for named fields.
func (s *Service) IsMultiField() bool {
	return s != nil && !s.singleField
}

func (s *Service) setInitialFieldPipelineDescriptor(fieldName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.fieldPipelineDescriptors[fieldName]; exists {
		return
	}
	s.fieldPipelineDescriptors[fieldName] = s.pipelineForField(fieldName).Descriptor()
}

// KeyGeneratorDescriptor returns the identity of the service key generator.
func (s *Service) KeyGeneratorDescriptor() (KeyGeneratorDescriptor, bool) {
	if s == nil || s.keyGenerator == nil {
		return KeyGeneratorDescriptor{}, false
	}
	return *s.keyGenerator, true
}

// PipelineDescriptors returns the validated pipeline identities observed for
// each indexed field.
func (s *Service) PipelineDescriptors() (map[string]PipelineDescriptor, error) {
	if s == nil {
		return nil, fmt.Errorf("fts: nil service")
	}

	fields := s.fieldNames()
	result := make(map[string]PipelineDescriptor, len(fields))
	for _, fieldName := range fields {
		descriptor, ok := s.fieldPipelineDescriptor(fieldName)
		if !ok {
			return nil, fmt.Errorf("fts: field %q: pipeline descriptor is unavailable", fieldName)
		}
		if err := ValidatePipelineDescriptor(descriptor); err != nil {
			return nil, fmt.Errorf("fts: field %q: %w", fieldName, err)
		}
		result[fieldName] = descriptor
	}
	if err := s.ValidatePipelineDescriptors(result); err != nil {
		return nil, fmt.Errorf("fts: configured pipelines do not match indexed fields: %w", err)
	}
	return result, nil
}

// ValidatePipelineDescriptors checks the pipelines configured for the fields
// against persisted pipeline identities.
func (s *Service) ValidatePipelineDescriptors(expected map[string]PipelineDescriptor) error {
	if s == nil {
		return fmt.Errorf("fts: nil service")
	}
	actual := make(map[string]PipelineDescriptor, len(s.fieldNames()))
	for _, fieldName := range s.fieldNames() {
		descriptor := s.pipelineForField(fieldName).Descriptor()
		if err := ValidatePipelineDescriptor(descriptor); err != nil {
			return fmt.Errorf("fts: field %q: invalid configured pipeline descriptor: %w", fieldName, err)
		}
		actual[fieldName] = descriptor
	}
	return ValidatePipelineDescriptors(expected, actual)
}

func (s *Service) fieldPipelineDescriptor(fieldName string) (PipelineDescriptor, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	descriptor, ok := s.fieldPipelineDescriptors[fieldName]
	return descriptor, ok
}

func (s *Service) recordFieldPipeline(fieldName string, pipeline Pipeline) error {
	if pipeline == nil {
		return fmt.Errorf("fts: field %q: nil pipeline", fieldName)
	}
	descriptor := pipeline.Descriptor()
	if err := ValidatePipelineDescriptor(descriptor); err != nil {
		return fmt.Errorf("fts: field %q: invalid pipeline descriptor: %w", fieldName, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if previous, exists := s.fieldPipelineDescriptors[fieldName]; exists {
		if err := ValidatePipelineDescriptors(
			map[string]PipelineDescriptor{fieldName: previous},
			map[string]PipelineDescriptor{fieldName: descriptor},
		); err != nil {
			return err
		}
	}
	s.fieldPipelineDescriptors[fieldName] = descriptor
	return nil
}

// pipelineForField reads pipeline configuration fixed during construction.
func (s *Service) pipelineForField(fieldName string) Pipeline {
	if pipeline := s.fieldPipelines[fieldName]; pipeline != nil {
		return pipeline
	}
	return s.pipeline
}

func newService(keyGen KeyGenerator, opts ...*Option) *Service {
	s := &Service{
		keyGen:                   keyGen,
		pipeline:                 defaultPipeline{},
		indexes:                  make(map[string]Index),
		fieldPipelines:           make(map[string]Pipeline),
		fieldPipelineDescriptors: make(map[string]PipelineDescriptor),
		collection:               newCollectionStats(),
		registry:                 NewDocRegistry(),
		tombstones:               NewTombstones(),
		autoCompactionCheck:      true,
	}

	for _, opt := range opts {
		if opt != nil && opt.apply != nil {
			opt.apply(s)
		}
	}

	if s.keyGen == nil {
		s.keyGen = WordKeys
	}
	if descriptor, ok := defaultKeyGeneratorDescriptor(s.keyGen); ok && s.keyGenerator == nil {
		s.keyGenerator = &descriptor
	}

	s.finalizeRestoreState()

	return s
}

func (s *Service) Delete(docID DocID) bool {
	return s.delete(docID, true)
}

func (s *Service) delete(docID DocID, checkCompaction bool) bool {
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
	if checkCompaction {
		s.maybeTriggerCompactionCheck()
	}
	return true
}

func (s *Service) Update(ctx context.Context, doc Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if doc.ID == "" {
		return fmt.Errorf("fts: document id is empty")
	}
	s.delete(doc.ID, false)
	if err := s.Index(ctx, doc); err != nil {
		return err
	}
	s.maybeTriggerCompactionCheck()
	return nil
}

// SearchText parses and executes a query with a unified set of search options.
func (s *Service) SearchText(ctx context.Context, query string, options SearchOptions) (*SearchResult, error) {
	candidates, err := s.newCandidateSet(ctx, options.CandidateIDs)
	if err != nil {
		return nil, err
	}
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
	parsed = bindDefaultField(parsed, options.DefaultField)
	exec.observePreprocess(preStart)

	res, err := s.searchResultForQuery(
		ctx,
		candidates,
		parsed,
		options.MaxResults,
		optionalQueryFieldScope(options.Fields),
	)
	if err != nil {
		return nil, err
	}
	exec.observeTotal(start)
	return attachDiagnostics(ctx, res), nil
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

func (s *Service) CompactionStats() CompactionStats {
	if s == nil {
		return CompactionStats{}
	}
	totalAssigned := 0
	liveDocs := 0
	tombstonedDocs := 0
	if s.registry != nil {
		totalAssigned = s.registry.TotalAssigned()
		liveDocs = s.registry.ActiveLen()
	}
	if s.tombstones != nil {
		tombstonedDocs = s.tombstones.Count()
	}
	stats := CompactionStats{
		TotalAssignedDocs: totalAssigned,
		LiveDocs:          liveDocs,
		TombstonedDocs:    tombstonedDocs,
	}
	if totalAssigned > 0 {
		stats.TombstoneLoadFactor = float64(tombstonedDocs) / float64(totalAssigned)
	}
	return stats
}

func (s *Service) NeedsCompaction() bool {
	if s == nil || s.compactionLoadFactor <= 0 {
		return false
	}
	return s.CompactionStats().TombstoneLoadFactor >= s.compactionLoadFactor
}

func (s *Service) maybeTriggerCompactionCheck() {
	if s == nil || !s.autoCompactionCheck {
		return
	}
	if !s.NeedsCompaction() {
		return
	}
	if s.compactionCallback != nil {
		s.compactionCallback(s.CompactionStats())
	}
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
