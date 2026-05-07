package fts

import (
	"context"
	"fmt"
	"sort"
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

	index, err := s.getOrCreateIndex(name)
	if err != nil {
		return fmt.Errorf("fts: index document %q: %w", docID, err)
	}

	pipeline := field.Pipeline
	if pipeline == nil {
		pipeline = s.pipeline
	}

	positional, hasPositions := index.(PositionalIndex)
	tokens := pipeline.Process(field.Value)
	if s.scorer != nil {
		s.collection.observe(name, docID, uint32(len(tokens)))
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
			if hasPositions {
				if err := positional.InsertAt(key, docID, uint32(pos)); err != nil {
					return fmt.Errorf("fts: index document %q field %q: insert: %w", docID, name, err)
				}
				continue
			}
			if err := index.Insert(key, docID); err != nil {
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
	sort.Strings(names)
	return names
}

func (s *Service) resolveFields(explicit string) []string {
	return s.resolveScopedFields(explicit, queryFieldScope{})
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

func (s *Service) searchPhraseFieldsResult(ctx context.Context, fields []string, phrase string, maxResults int) (*SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, exec := ensureDiagnosticsContext(ctx)
	exec.setQueryTypeIfEmpty("phrase")

	start := exec.startTimer()

	preStart := exec.startTimer()
	plan := s.preparePhrase(fields, phrase)
	exec.observePreprocess(preStart)
	exec.addFields(len(fields))
	exec.addTokens(len(plan.tokens))

	if len(plan.tokens) == 0 {
		exec.setSearchTokensTiming(0)
		exec.observeTotal(start)
		return attachDiagnostics(ctx, &SearchResult{Results: []Result{}}), nil
	}
	if plan.fallback != nil {
		res, err := s.searchResultForQuery(ctx, *plan.fallback, maxResults, newQueryFieldScope(fields))
		if err != nil {
			return nil, err
		}
		exec.observeTotal(start)
		return attachDiagnostics(ctx, res), nil
	}
	exec.setStrategy("phrase_exact")

	searchStart := exec.startTimer()
	hits, err := s.evalExactPhraseTokenHits(ctx, fields, plan.tokens)
	if err != nil {
		return nil, err
	}

	exec.observeSearchTokens(searchStart)
	exec.observeTotal(start)
	return attachDiagnostics(ctx, searchResultFromHits(hits, maxResults, s.scorer != nil)), nil
}

func (s *Service) searchPhraseNearFieldsResult(ctx context.Context, fields []string, phrase string, distance int, maxResults int) (*SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, exec := ensureDiagnosticsContext(ctx)
	exec.setQueryTypeIfEmpty("phrase_near")
	if distance < 0 {
		return nil, fmt.Errorf("fts: phrase near search: negative distance %d", distance)
	}

	start := exec.startTimer()

	preStart := exec.startTimer()
	plan := s.preparePhrase(fields, phrase)
	exec.observePreprocess(preStart)
	exec.addFields(len(fields))
	exec.addTokens(len(plan.tokens))

	if len(plan.tokens) == 0 {
		exec.setSearchTokensTiming(0)
		exec.observeTotal(start)
		return attachDiagnostics(ctx, &SearchResult{Results: []Result{}}), nil
	}
	if plan.fallback != nil {
		res, err := s.searchResultForQuery(ctx, *plan.fallback, maxResults, newQueryFieldScope(fields))
		if err != nil {
			return nil, err
		}
		exec.observeTotal(start)
		return attachDiagnostics(ctx, res), nil
	}
	exec.setStrategy("phrase_near")

	searchStart := exec.startTimer()
	hits, err := s.evalNearPhraseTokenHits(ctx, fields, plan.tokens, uint32(distance))
	if err != nil {
		return nil, err
	}

	exec.observeSearchTokens(searchStart)
	exec.observeTotal(start)
	return attachDiagnostics(ctx, searchResultFromHits(hits, maxResults, s.scorer != nil)), nil
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
