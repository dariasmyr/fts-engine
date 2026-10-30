package fts

import (
	"context"
	"fmt"
	"slices"
)

func (s *Service) Index(ctx context.Context, doc Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if doc.ID == "" {
		return fmt.Errorf("fts: document id is empty")
	}
	if len(doc.Fields) == 0 {
		return fmt.Errorf("fts: document %q has no fields", doc.ID)
	}

	for name, field := range doc.Fields {
		if err := s.indexField(ctx, doc.ID, name, field); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) indexField(ctx context.Context, docID DocID, name string, field Field) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.singleField && name != DefaultField {
		return fmt.Errorf("fts: index document %q: field %q is not available in single-field mode; use %q or fts.NewMultiField", docID, name, DefaultField)
	}

	pipeline := s.pipelineForField(name)
	if err := s.recordFieldPipeline(name, pipeline); err != nil {
		return fmt.Errorf("fts: index document %q: %w", docID, err)
	}

	index, err := s.getOrCreateIndex(name)
	if err != nil {
		return fmt.Errorf("fts: index document %q: %w", docID, err)
	}

	positional, supportsPositions := index.(MutablePositionalIndex)
	mutable, supportsInsert := index.(MutableIndex)
	if !supportsPositions && !supportsInsert {
		return fmt.Errorf("fts: index document %q: field %q index is read-only", docID, name)
	}
	tokens := pipeline.Process(field.Value)
	ord := s.registry.GetOrAssign(docID)
	if s.scorer != nil {
		s.collection.observe(name, ord, uint32(len(tokens)))
	}
	for pos, token := range tokens {
		if err := ctx.Err(); err != nil {
			return err
		}

		keys, err := s.keyGen(token)
		if err != nil {
			return fmt.Errorf("fts: index document %q field %q: keygen: %w", docID, name, err)
		}

		for _, key := range keys {
			if s.filter != nil {
				if ok := s.filter.Add([]byte(key)); !ok {
					return fmt.Errorf("fts: index document %q field %q: filter add failed for key %q", docID, name, key)
				}
			}
			if supportsPositions {
				if err := positional.InsertAt(key, uint32(pos), ord); err != nil {
					return fmt.Errorf("fts: index document %q field %q: insert: %w", docID, name, err)
				}
				continue
			}
			if err := mutable.Insert(key, ord); err != nil {
				return fmt.Errorf("fts: index document %q field %q: insert: %w", docID, name, err)
			}
		}
	}

	return nil
}

func (s *Service) Fields() []string {
	return s.fieldNames()
}

func (s *Service) fieldNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0, len(s.indexes))
	for name := range s.indexes {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func (s *Service) lookupIndex(name string) (Index, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	idx, ok := s.indexes[name]
	return idx, ok
}

func (s *Service) getOrCreateIndex(name string) (Index, error) {
	if idx, ok := s.lookupIndex(name); ok {
		return idx, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if idx, ok := s.indexes[name]; ok {
		return idx, nil
	}
	if s.indexFactory == nil {
		return nil, fmt.Errorf("fts: no index factory configured for field %q", name)
	}

	idx, err := s.indexFactory(name)
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return nil, fmt.Errorf("fts: index factory returned nil for field %q", name)
	}

	s.indexes[name] = idx
	return idx, nil
}

func (s *Service) SnapshotFields() (map[string]Index, Filter) {
	if s == nil {
		return nil, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	fields := make(map[string]Index, len(s.indexes))
	for name, idx := range s.indexes {
		fields[name] = idx
	}
	return fields, s.filter
}
