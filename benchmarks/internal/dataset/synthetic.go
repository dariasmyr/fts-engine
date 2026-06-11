package dataset

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/harness"
	"github.com/dariasmyr/fts-engine/benchmarks/internal/quality"
)

type Corpus struct {
	Docs    []harness.Document
	Queries []harness.Query
	Qrels   quality.Qrels
}

type SyntheticConfig struct {
	NumDocs       int
	NumQueries    int
	WordsPerDoc   int
	WordsPerQuery int
	VocabSize     int
	ZipfS         float64
	K             int
	Seed          uint64
}

func Synthetic(cfg SyntheticConfig) *Corpus {
	if cfg.Seed == 0 {
		cfg.Seed = 0xC0FFEE
	}
	rng := rand.New(rand.NewSource(int64(cfg.Seed)))
	zipf := rand.NewZipf(rng, cfg.ZipfS, 1, uint64(cfg.VocabSize-1))
	vocab := make([]string, cfg.VocabSize)
	for i := range vocab {
		vocab[i] = fmt.Sprintf("tok%06d", i)
	}

	postings := make(map[string][]string, cfg.VocabSize)
	docs := make([]harness.Document, cfg.NumDocs)
	for i := range docs {
		docID := fmt.Sprintf("syn-%d", i)
		terms := make([]string, cfg.WordsPerDoc)
		seen := make(map[string]struct{}, cfg.WordsPerDoc)
		for j := range terms {
			term := vocab[int(zipf.Uint64())]
			terms[j] = term
			seen[term] = struct{}{}
		}
		for term := range seen {
			postings[term] = append(postings[term], docID)
		}
		docs[i] = harness.Document{ID: docID, Body: strings.Join(terms, " ")}
	}

	queries := make([]harness.Query, cfg.NumQueries)
	qrels := make(quality.Qrels, cfg.NumQueries)
	for i := range queries {
		queryID := fmt.Sprintf("q-%d", i)
		terms := make([]string, cfg.WordsPerQuery)
		relevant := make(map[string]int)
		for j := range terms {
			term := vocab[int(zipf.Uint64())]
			terms[j] = term
			for _, docID := range postings[term] {
				relevant[docID] = 1
			}
		}
		queries[i] = harness.Query{ID: queryID, Text: strings.Join(terms, " "), K: cfg.K}
		if len(relevant) > 0 {
			qrels[queryID] = relevant
		}
	}

	for term := range postings {
		sort.Strings(postings[term])
	}

	return &Corpus{Docs: docs, Queries: queries, Qrels: qrels}
}
