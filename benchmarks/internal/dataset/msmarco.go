package dataset

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/harness"
	"github.com/dariasmyr/fts-engine/benchmarks/internal/quality"
)

type MSMARCOConfig struct {
	Dir        string
	MaxDocs    int
	MaxQueries int
	K          int
	Seed       uint64
}

func LoadMSMARCO(cfg MSMARCOConfig) (*Corpus, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("dataset: msmarco dir is empty")
	}
	if cfg.Seed == 0 {
		cfg.Seed = 0xC0FFEE
	}
	qrels, err := readQrels(filepath.Join(cfg.Dir, "qrels.dev.small.tsv"))
	if err != nil {
		return nil, fmt.Errorf("dataset: msmarco qrels: %w", err)
	}
	queriesRaw, err := readQueryTSV(filepath.Join(cfg.Dir, "queries.dev.small.tsv"), cfg.K)
	if err != nil {
		return nil, fmt.Errorf("dataset: msmarco queries: %w", err)
	}

	queries := queriesRaw[:0]
	for _, q := range queriesRaw {
		if _, ok := qrels[q.ID]; !ok {
			continue
		}
		queries = append(queries, q)
		if cfg.MaxQueries > 0 && len(queries) >= cfg.MaxQueries {
			break
		}
	}

	mustHave := make(map[string]struct{})
	for _, q := range queries {
		for docID := range qrels[q.ID] {
			mustHave[docID] = struct{}{}
		}
	}

	docs, err := readCollectionSampled(filepath.Join(cfg.Dir, "collection.tsv"), mustHave, cfg.MaxDocs, cfg.Seed)
	if err != nil {
		return nil, fmt.Errorf("dataset: msmarco collection: %w", err)
	}

	docSet := make(map[string]struct{}, len(docs))
	for _, doc := range docs {
		docSet[doc.ID] = struct{}{}
	}

	filteredQrels := make(quality.Qrels, len(queries))
	keptQueries := make([]harness.Query, 0, len(queries))
	for _, q := range queries {
		filtered := make(map[string]int)
		for docID, rel := range qrels[q.ID] {
			if _, ok := docSet[docID]; ok {
				filtered[docID] = rel
			}
		}
		if len(filtered) == 0 {
			continue
		}
		filteredQrels[q.ID] = filtered
		keptQueries = append(keptQueries, q)
	}

	return &Corpus{Docs: docs, Queries: keptQueries, Qrels: filteredQrels}, nil
}

func readQueryTSV(path string, k int) ([]harness.Query, error) {
	f, err := openMaybeGz(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := make([]harness.Query, 0, 1024)
	sc := newLargeScanner(f)
	for sc.Scan() {
		line := sc.Text()
		tab := strings.IndexByte(line, '\t')
		if tab < 0 {
			continue
		}
		out = append(out, harness.Query{ID: line[:tab], Text: line[tab+1:], K: k})
	}
	return out, sc.Err()
}

func readQrels(path string) (quality.Qrels, error) {
	f, err := openMaybeGz(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := make(quality.Qrels)
	sc := newLargeScanner(f)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), "\t")
		if len(fields) < 4 {
			continue
		}
		rel, err := strconv.Atoi(fields[3])
		if err != nil || rel <= 0 {
			continue
		}
		qid, docID := fields[0], fields[2]
		if out[qid] == nil {
			out[qid] = make(map[string]int)
		}
		out[qid][docID] = rel
	}
	return out, sc.Err()
}

func readCollectionSampled(path string, mustHave map[string]struct{}, maxDocs int, seed uint64) ([]harness.Document, error) {
	f, err := openMaybeGz(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if maxDocs == 0 {
		out := make([]harness.Document, 0, 1024)
		sc := newLargeScanner(f)
		for sc.Scan() {
			if doc, ok := parseCollectionLine(sc.Text()); ok {
				out = append(out, doc)
			}
		}
		return out, sc.Err()
	}

	rng := rand.New(rand.NewSource(int64(seed)))
	forced := make([]harness.Document, 0, len(mustHave))
	reservoirCap := maxDocs - len(mustHave)
	if reservoirCap < 0 {
		reservoirCap = 0
	}
	reservoir := make([]harness.Document, 0, reservoirCap)

	sc := newLargeScanner(f)
	seen := 0
	for sc.Scan() {
		doc, ok := parseCollectionLine(sc.Text())
		if !ok {
			continue
		}
		if _, ok := mustHave[doc.ID]; ok {
			forced = append(forced, doc)
			continue
		}
		seen++
		if reservoirCap == 0 {
			continue
		}
		if len(reservoir) < reservoirCap {
			reservoir = append(reservoir, doc)
			continue
		}
		j := rng.Intn(seen)
		if j < reservoirCap {
			reservoir[j] = doc
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return append(forced, reservoir...), nil
}

func parseCollectionLine(line string) (harness.Document, bool) {
	tab := strings.IndexByte(line, '\t')
	if tab < 0 {
		return harness.Document{}, false
	}
	return harness.Document{ID: line[:tab], Body: line[tab+1:]}, true
}

func newLargeScanner(r io.Reader) *bufio.Scanner {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	return sc
}

func openMaybeGz(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		return &gzReadCloser{gz: gz, f: f}, nil
	}
	return f, nil
}

type gzReadCloser struct {
	gz *gzip.Reader
	f  *os.File
}

func (g *gzReadCloser) Read(p []byte) (int, error) { return g.gz.Read(p) }

func (g *gzReadCloser) Close() error {
	if err := g.gz.Close(); err != nil {
		_ = g.f.Close()
		return err
	}
	return g.f.Close()
}
