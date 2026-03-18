package fts

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
	"unicode"
)

type Service struct {
	index             Index
	keyGen            KeyGenerator
	pipeline          Pipeline
	durationFormatter func(time.Duration) string
}

func New(index Index, keyGen KeyGenerator, opts ...Option) *Service {
	s := &Service{
		index:             index,
		keyGen:            keyGen,
		pipeline:          defaultPipeline{},
		durationFormatter: formatDuration,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}

	if s.keyGen == nil {
		s.keyGen = WordKeys
	}

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

	tokens := s.pipeline.Process(content)
	for _, token := range tokens {
		if err := ctx.Err(); err != nil {
			return err
		}

		keys, err := s.keyGen(token)
		if err != nil {
			return fmt.Errorf("fts: index document: keygen: %w", err)
		}

		for _, key := range keys {
			if err := s.index.Insert(key, docID); err != nil {
				return fmt.Errorf("fts: index document: insert: %w", err)
			}
		}
	}

	return nil
}

func (s *Service) SearchDocuments(ctx context.Context, query string, maxResults int) (*SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	start := time.Now()
	timings := make(map[string]string, 3)

	preStart := time.Now()
	tokens := s.pipeline.Process(query)
	timings["preprocess"] = s.durationFormatter(time.Since(preStart))

	searchStart := time.Now()
	uniqueMatches := make(map[DocID]int)
	totalMatches := make(map[DocID]int)

	for _, token := range tokens {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		keys, err := s.keyGen(token)
		if err != nil {
			return nil, fmt.Errorf("fts: search: keygen: %w", err)
		}

		for _, key := range keys {
			docs, err := s.index.Search(key)
			if err != nil {
				return nil, fmt.Errorf("fts: search: index search: %w", err)
			}

			for _, doc := range docs {
				uniqueMatches[doc.ID]++
				totalMatches[doc.ID] += int(doc.Count)
			}
		}
	}

	timings["search_tokens"] = s.durationFormatter(time.Since(searchStart))

	results := make([]Result, 0, len(uniqueMatches))
	for id, unique := range uniqueMatches {
		results = append(results, Result{
			ID:            id,
			UniqueMatches: unique,
			TotalMatches:  totalMatches[id],
		})
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].UniqueMatches != results[j].UniqueMatches {
			return results[i].UniqueMatches > results[j].UniqueMatches
		}
		if results[i].TotalMatches != results[j].TotalMatches {
			return results[i].TotalMatches > results[j].TotalMatches
		}
		return results[i].ID < results[j].ID
	})

	totalFound := len(results)
	if maxResults <= 0 || maxResults > totalFound {
		maxResults = totalFound
	}

	timings["total"] = s.durationFormatter(time.Since(start))

	return &SearchResult{
		Results:           results[:maxResults],
		TotalResultsCount: totalFound,
		Timings:           timings,
	}, nil
}

func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dus", d.Microseconds())
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}

type defaultPipeline struct{}

func (defaultPipeline) Process(text string) []string {
	if text == "" {
		return nil
	}

	tokens := make([]string, 0, 16)
	var b strings.Builder

	flush := func() {
		if b.Len() == 0 {
			return
		}
		tokens = append(tokens, b.String())
		b.Reset()
	}

	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
			continue
		}
		flush()
	}
	flush()

	return tokens
}
