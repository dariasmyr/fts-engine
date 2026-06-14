package dataset

import (
	"compress/bzip2"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/harness"
	"github.com/dariasmyr/fts-engine/benchmarks/internal/quality"
)

const (
	queryClassTerm   = "term"
	queryClassAndHH  = "and-hh"
	queryClassAndHL  = "and-hl"
	queryClassOrHH   = "or-hh"
	queryClassPhrase = "phrase"
	queryClassPrefix = "prefix"
)

type WikiTypedConfig struct {
	DumpPath         string
	CacheDir         string
	MaxDocs          int
	K                int
	Seed             uint64
	QueriesPerClass  int
	HighSkipTop      int
	HighPool         int
	LowPool          int
	PrefixMinExpand  int
	PrefixMaxExpand  int
	QueryTypes       []string
	IncludeTitleText bool
	Logf             func(format string, args ...any)
}

type wikiPage struct {
	Title    string      `xml:"title"`
	ID       string      `xml:"id"`
	Redirect *struct{}   `xml:"redirect"`
	Revision wikiVersion `xml:"revision"`
}

type wikiVersion struct {
	Text string `xml:"text"`
}

type corpusStats struct {
	postings     map[string][]int
	phrases      map[string][]int
	prefixTerms  map[string][]string
	docIDs       []string
	termFreqs    []termStat
	prefixStats  []prefixStat
	highTerms    []string
	lowTerms     []string
	termUniverse []string
}

type termStat struct {
	Term string
	DF   int
}

type prefixStat struct {
	Prefix     string
	ExpansionN int
}

type docsCacheResult struct {
	docs      []harness.Document
	cachePath string
	cacheUsed bool
	cacheKey  string
}

type wikiTypedDocsCacheFile struct {
	Version string             `json:"version"`
	Key     string             `json:"key"`
	Docs    []harness.Document `json:"docs"`
}

type wikiTypedQueriesCacheFile struct {
	Version string             `json:"version"`
	Key     string             `json:"key"`
	Groups  []cachedQueryGroup `json:"groups"`
}

type wikiTypedQrelsCacheFile struct {
	Version string             `json:"version"`
	Key     string             `json:"key"`
	Groups  []cachedQrelsGroup `json:"groups"`
}

type wikiTypedManifestFile struct {
	Version     string                  `json:"version"`
	DumpPath    string                  `json:"dump_path"`
	CacheDir    string                  `json:"cache_dir"`
	DocsKey     string                  `json:"docs_key"`
	QueryKey    string                  `json:"query_key"`
	DocsFile    string                  `json:"docs_file"`
	QueriesFile string                  `json:"queries_file"`
	QrelsFile   string                  `json:"qrels_file"`
	Params      map[string]any          `json:"params"`
	Files       map[string]cacheFileRef `json:"files"`
}

type cacheFileRef struct {
	Path string `json:"path"`
	Kind string `json:"kind"`
}

type cachedQueryGroup struct {
	Name    string          `json:"name"`
	Queries []harness.Query `json:"queries"`
}

type cachedQrelsGroup struct {
	Name  string        `json:"name"`
	Qrels quality.Qrels `json:"qrels"`
}

const wikiTypedCacheVersion = "wiki-typed.v2"

func LoadWikiTyped(cfg WikiTypedConfig) (*Corpus, error) {
	if cfg.DumpPath == "" {
		return nil, fmt.Errorf("dataset: wiki-typed dump path is empty")
	}
	if cfg.Seed == 0 {
		cfg.Seed = 0xC0FFEE
	}
	if cfg.QueriesPerClass <= 0 {
		cfg.QueriesPerClass = 200
	}
	if cfg.HighSkipTop < 0 {
		cfg.HighSkipTop = 30
	}
	if cfg.HighPool <= 0 {
		cfg.HighPool = 300
	}
	if cfg.LowPool <= 0 {
		cfg.LowPool = 5000
	}
	if cfg.PrefixMinExpand <= 0 {
		cfg.PrefixMinExpand = 2
	}
	if cfg.PrefixMaxExpand <= 0 {
		cfg.PrefixMaxExpand = 32
	}
	if cfg.CacheDir == "" {
		cfg.CacheDir = filepath.Join(filepath.Dir(cfg.DumpPath), ".wiki-typed-cache")
	}
	cfg.logf("wiki-typed: load start dump=%s cache_dir=%s max_docs=%d typed_queries=%d", cfg.DumpPath, cfg.CacheDir, cfg.MaxDocs, cfg.QueriesPerClass)

	docsResult, err := loadWikiDocuments(cfg)
	if err != nil {
		return nil, err
	}
	docs := docsResult.docs
	cfg.logf("wiki-typed: documents ready count=%d docs_cache_used=%t", len(docs), docsResult.cacheUsed)
	cacheUsed := false
	queriesPath, cacheKey, err := wikiTypedQueriesCacheLocation(cfg)
	if err != nil {
		return nil, err
	}
	qrelsPath, _, err := wikiTypedQrelsCacheLocation(cfg)
	if err != nil {
		return nil, err
	}
	manifestPath, err := wikiTypedManifestLocation(cfg)
	if err != nil {
		return nil, err
	}
	groups, err := loadWikiTypedCachedGroups(queriesPath, qrelsPath, cacheKey)
	if err == nil {
		cacheUsed = true
		cfg.logf("wiki-typed: queries cache hit file=%s", queriesPath)
		cfg.logf("wiki-typed: qrels cache hit file=%s", qrelsPath)
	} else {
		cfg.logf("wiki-typed: queries/qrels cache miss files=%s,%s", queriesPath, qrelsPath)
		cfg.logf("wiki-typed: building corpus stats")
		stats := buildCorpusStats(docs, cfg)
		allCfg := cfg
		allCfg.QueryTypes = nil
		cfg.logf("wiki-typed: generating query groups")
		groups, err = buildWikiQueryGroups(stats, allCfg)
		if err != nil {
			return nil, err
		}
		if saveErr := saveWikiTypedCachedGroups(queriesPath, qrelsPath, cacheKey, groups); saveErr != nil {
			return nil, saveErr
		}
		cfg.logf("wiki-typed: saved queries cache file=%s groups=%d", queriesPath, len(groups))
		cfg.logf("wiki-typed: saved qrels cache file=%s groups=%d", qrelsPath, len(groups))
	}
	if err := saveWikiTypedManifest(manifestPath, docsResult, queriesPath, qrelsPath, cacheKey, cfg); err != nil {
		return nil, err
	}
	cfg.logf("wiki-typed: manifest ready file=%s", manifestPath)
	groups = applyWikiTypedQuerySelection(groups, cfg.QueryTypes, cfg.K)
	if len(groups) == 0 {
		return nil, fmt.Errorf("dataset: wiki-typed no query classes selected")
	}

	allQueries := make([]harness.Query, 0)
	for _, group := range groups {
		allQueries = append(allQueries, group.Queries...)
	}
	cfg.logf("wiki-typed: selected query groups=%d total_queries=%d query_cache_used=%t", len(groups), len(allQueries), cacheUsed)

	return &Corpus{
		Docs:    docs,
		Queries: allQueries,
		Qrels:   quality.Qrels{},
		Groups:  groups,
		Meta: map[string]any{
			"cache_dir":          cfg.CacheDir,
			"docs_cache_file":    docsResult.cachePath,
			"docs_cache_key":     docsResult.cacheKey,
			"docs_cache_used":    docsResult.cacheUsed,
			"manifest_file":      manifestPath,
			"queries_cache_file": queriesPath,
			"qrels_cache_file":   qrelsPath,
			"query_cache_key":    cacheKey,
			"query_cache_used":   cacheUsed,
		},
	}, nil
}

func loadWikiDocuments(cfg WikiTypedConfig) (docsCacheResult, error) {
	cachePath, cacheKey, err := wikiTypedDocsCacheLocation(cfg)
	if err != nil {
		return docsCacheResult{}, err
	}
	if docs, err := loadWikiTypedCachedDocs(cachePath, cacheKey); err == nil {
		cfg.logf("wiki-typed: docs cache hit file=%s", cachePath)
		return docsCacheResult{docs: docs, cachePath: cachePath, cacheUsed: true, cacheKey: cacheKey}, nil
	}
	cfg.logf("wiki-typed: docs cache miss file=%s", cachePath)
	cfg.logf("wiki-typed: parsing dump %s", cfg.DumpPath)

	r, err := openMaybeCompressed(cfg.DumpPath)
	if err != nil {
		return docsCacheResult{}, err
	}
	defer r.Close()

	dec := xml.NewDecoder(r)
	docs := make([]harness.Document, 0, max(1024, cfg.MaxDocs))
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return docsCacheResult{}, fmt.Errorf("dataset: wiki-typed parse XML: %w", err)
		}
		start, ok := tok.(xml.StartElement)
		if !ok || start.Name.Local != "page" {
			continue
		}
		var page wikiPage
		if err := dec.DecodeElement(&page, &start); err != nil {
			return docsCacheResult{}, fmt.Errorf("dataset: wiki-typed decode page: %w", err)
		}
		if page.Redirect != nil {
			continue
		}
		body := strings.TrimSpace(page.Revision.Text)
		if cfg.IncludeTitleText && page.Title != "" {
			body = strings.TrimSpace(page.Title + "\n" + body)
		}
		if body == "" {
			continue
		}
		docID := strings.TrimSpace(page.ID)
		if docID == "" {
			docID = strings.TrimSpace(page.Title)
		}
		if docID == "" {
			continue
		}
		docs = append(docs, harness.Document{ID: docID, Body: body})
		if cfg.MaxDocs > 0 && len(docs) >= cfg.MaxDocs {
			break
		}
	}
	if len(docs) == 0 {
		return docsCacheResult{}, fmt.Errorf("dataset: wiki-typed no documents loaded from %s", cfg.DumpPath)
	}
	if err := saveWikiTypedCachedDocs(cachePath, cacheKey, docs); err != nil {
		return docsCacheResult{}, err
	}
	cfg.logf("wiki-typed: saved docs cache file=%s docs=%d", cachePath, len(docs))
	return docsCacheResult{docs: docs, cachePath: cachePath, cacheUsed: false, cacheKey: cacheKey}, nil
}

func (cfg WikiTypedConfig) logf(format string, args ...any) {
	if cfg.Logf != nil {
		cfg.Logf(format, args...)
	}
}

func buildCorpusStats(docs []harness.Document, cfg WikiTypedConfig) corpusStats {
	postings := make(map[string][]int, len(docs)*2)
	phrases := make(map[string][]int, len(docs)*2)
	docIDs := make([]string, len(docs))
	for docIdx, doc := range docs {
		docIDs[docIdx] = doc.ID
		tokens := tokenizeWikiText(doc.Body)
		if len(tokens) == 0 {
			continue
		}
		seenTerms := make(map[string]struct{}, len(tokens))
		seenPhrases := make(map[string]struct{}, len(tokens))
		for i, token := range tokens {
			seenTerms[token] = struct{}{}
			if i+1 < len(tokens) {
				phrase := token + " " + tokens[i+1]
				seenPhrases[phrase] = struct{}{}
			}
		}
		for term := range seenTerms {
			postings[term] = append(postings[term], docIdx)
		}
		for phrase := range seenPhrases {
			phrases[phrase] = append(phrases[phrase], docIdx)
		}
	}

	termFreqs := make([]termStat, 0, len(postings))
	for term, docs := range postings {
		termFreqs = append(termFreqs, termStat{Term: term, DF: len(docs)})
	}
	sort.Slice(termFreqs, func(i, j int) bool {
		if termFreqs[i].DF == termFreqs[j].DF {
			return termFreqs[i].Term < termFreqs[j].Term
		}
		return termFreqs[i].DF > termFreqs[j].DF
	})

	highTerms, lowTerms, universe := selectTermBuckets(termFreqs, cfg)
	prefixStats, prefixTerms := buildPrefixStats(postings, cfg)

	return corpusStats{
		postings:     postings,
		phrases:      phrases,
		prefixTerms:  prefixTerms,
		docIDs:       docIDs,
		termFreqs:    termFreqs,
		prefixStats:  prefixStats,
		highTerms:    highTerms,
		lowTerms:     lowTerms,
		termUniverse: universe,
	}
}

func buildWikiQueryGroups(stats corpusStats, cfg WikiTypedConfig) ([]QueryGroup, error) {
	rng := rand.New(rand.NewSource(int64(cfg.Seed)))
	requested := normalizeQueryTypeFilter(cfg.QueryTypes)
	builders := []struct {
		name  string
		build func(*rand.Rand, corpusStats, WikiTypedConfig) ([]harness.Query, error)
	}{
		{name: queryClassTerm, build: buildTermQueries},
		{name: queryClassAndHH, build: buildAndHHQueries},
		{name: queryClassAndHL, build: buildAndHLQueries},
		{name: queryClassOrHH, build: buildOrHHQueries},
		{name: queryClassPhrase, build: buildPhraseQueries},
		{name: queryClassPrefix, build: buildPrefixQueries},
	}
	groups := make([]QueryGroup, 0, len(builders))
	for _, builder := range builders {
		if len(requested) > 0 && !requested[builder.name] {
			continue
		}
		queries, err := builder.build(rng, stats, cfg)
		if err != nil {
			return nil, err
		}
		if len(queries) == 0 {
			return nil, fmt.Errorf("dataset: wiki-typed no queries generated for class %q", builder.name)
		}
		qrels, err := buildWikiTypedQrels(stats, queries)
		if err != nil {
			return nil, err
		}
		groups = append(groups, QueryGroup{Name: builder.name, Queries: queries, Qrels: qrels})
	}
	if len(groups) == 0 {
		return nil, fmt.Errorf("dataset: wiki-typed no query classes selected")
	}
	return groups, nil
}

func applyWikiTypedQuerySelection(groups []QueryGroup, requestedTypes []string, k int) []QueryGroup {
	requested := normalizeQueryTypeFilter(requestedTypes)
	out := make([]QueryGroup, 0, len(groups))
	for _, group := range groups {
		if len(requested) > 0 && !requested[group.Name] {
			continue
		}
		copiedQueries := make([]harness.Query, 0, len(group.Queries))
		for _, q := range group.Queries {
			q.K = k
			copiedQueries = append(copiedQueries, q)
		}
		out = append(out, QueryGroup{Name: group.Name, Queries: copiedQueries, Qrels: group.Qrels})
	}
	return out
}

func buildTermQueries(rng *rand.Rand, stats corpusStats, cfg WikiTypedConfig) ([]harness.Query, error) {
	pool := append([]string(nil), stats.highTerms...)
	pool = append(pool, stats.lowTerms...)
	if len(pool) == 0 {
		pool = append(pool, stats.termUniverse...)
	}
	pool = uniqueStrings(pool)
	return buildSimpleTermQueries(rng, pool, cfg.QueriesPerClass, cfg.K, queryClassTerm, harness.QueryKindTerm), nil
}

func buildPhraseQueries(rng *rand.Rand, stats corpusStats, cfg WikiTypedConfig) ([]harness.Query, error) {
	type phraseStat struct {
		Text string
		DF   int
	}
	phrases := make([]phraseStat, 0, len(stats.phrases))
	for phrase, docs := range stats.phrases {
		df := len(docs)
		if df < 2 {
			continue
		}
		phrases = append(phrases, phraseStat{Text: phrase, DF: df})
	}
	if len(phrases) == 0 {
		return nil, fmt.Errorf("dataset: wiki-typed phrase pool is empty")
	}
	sort.Slice(phrases, func(i, j int) bool {
		if phrases[i].DF == phrases[j].DF {
			return phrases[i].Text < phrases[j].Text
		}
		return phrases[i].DF > phrases[j].DF
	})
	if len(phrases) > cfg.LowPool {
		phrases = phrases[:cfg.LowPool]
	}
	queryTexts := sampleMappedStrings(rng, phrases, cfg.QueriesPerClass, func(p phraseStat) string { return p.Text })
	queries := make([]harness.Query, 0, len(queryTexts))
	for i, text := range queryTexts {
		queries = append(queries, harness.Query{ID: fmt.Sprintf("phrase-%03d", i), Kind: harness.QueryKindPhrase, Text: text, K: cfg.K, Class: queryClassPhrase})
	}
	return queries, nil
}

func buildPrefixQueries(rng *rand.Rand, stats corpusStats, cfg WikiTypedConfig) ([]harness.Query, error) {
	if len(stats.prefixStats) == 0 {
		return nil, fmt.Errorf("dataset: wiki-typed prefix pool is empty")
	}
	prefixes := sampleMappedStrings(rng, stats.prefixStats, cfg.QueriesPerClass, func(p prefixStat) string { return p.Prefix })
	queries := make([]harness.Query, 0, len(prefixes))
	for i, prefix := range prefixes {
		queries = append(queries, harness.Query{ID: fmt.Sprintf("prefix-%03d", i), Kind: harness.QueryKindPrefix, Text: prefix, K: cfg.K, Class: queryClassPrefix})
	}
	return queries, nil
}

func buildAndHHQueries(rng *rand.Rand, stats corpusStats, cfg WikiTypedConfig) ([]harness.Query, error) {
	return buildBooleanPairQueries(rng, stats.highTerms, stats.highTerms, stats, cfg, queryClassAndHH, true, harness.OccurMust)
}

func buildAndHLQueries(rng *rand.Rand, stats corpusStats, cfg WikiTypedConfig) ([]harness.Query, error) {
	return buildBooleanPairQueries(rng, stats.highTerms, stats.lowTerms, stats, cfg, queryClassAndHL, true, harness.OccurMust)
}

func buildOrHHQueries(rng *rand.Rand, stats corpusStats, cfg WikiTypedConfig) ([]harness.Query, error) {
	return buildBooleanPairQueries(rng, stats.highTerms, stats.highTerms, stats, cfg, queryClassOrHH, false, harness.OccurShould)
}

func buildBooleanPairQueries(rng *rand.Rand, leftPool, rightPool []string, stats corpusStats, cfg WikiTypedConfig, class string, requireIntersection bool, occur harness.BoolOccur) ([]harness.Query, error) {
	if len(leftPool) == 0 || len(rightPool) == 0 {
		return nil, fmt.Errorf("dataset: wiki-typed %s pool is empty", class)
	}
	seenPairs := make(map[string]struct{})
	queries := make([]harness.Query, 0, min(cfg.QueriesPerClass, len(leftPool)*max(1, len(rightPool))))
	maxAttempts := max(cfg.QueriesPerClass*50, 200)
	for attempts := 0; attempts < maxAttempts && len(queries) < cfg.QueriesPerClass; attempts++ {
		left := leftPool[rng.Intn(len(leftPool))]
		right := rightPool[rng.Intn(len(rightPool))]
		if left == right {
			continue
		}
		pairKey := orderedPairKey(left, right)
		if _, ok := seenPairs[pairKey]; ok {
			continue
		}
		if requireIntersection && !postingsIntersect(stats.postings[left], stats.postings[right]) {
			continue
		}
		seenPairs[pairKey] = struct{}{}
		queries = append(queries, harness.Query{
			ID:    fmt.Sprintf("%s-%03d", class, len(queries)),
			Kind:  harness.QueryKindBoolean,
			K:     cfg.K,
			Class: class,
			Boolean: &harness.BoolQuery{Clauses: []harness.BoolClause{
				{Occur: occur, Atom: harness.Atom{Kind: harness.QueryKindTerm, Text: left}},
				{Occur: occur, Atom: harness.Atom{Kind: harness.QueryKindTerm, Text: right}},
			}},
		})
	}
	if len(queries) == 0 {
		return nil, fmt.Errorf("dataset: wiki-typed failed to generate %s queries", class)
	}
	return queries, nil
}

func buildSimpleTermQueries(rng *rand.Rand, terms []string, limit int, k int, class string, kind harness.QueryKind) []harness.Query {
	selected := sampleStrings(rng, terms, limit)
	queries := make([]harness.Query, 0, len(selected))
	for i, term := range selected {
		queries = append(queries, harness.Query{ID: fmt.Sprintf("%s-%03d", class, i), Kind: kind, Text: term, K: k, Class: class})
	}
	return queries
}

func selectTermBuckets(termFreqs []termStat, cfg WikiTypedConfig) (high []string, low []string, universe []string) {
	if len(termFreqs) == 0 {
		return nil, nil, nil
	}
	start := min(cfg.HighSkipTop, len(termFreqs))
	highEnd := min(start+cfg.HighPool, len(termFreqs))
	lowEnd := min(highEnd+cfg.LowPool, len(termFreqs))
	for _, stat := range termFreqs[start:highEnd] {
		high = append(high, stat.Term)
	}
	for _, stat := range termFreqs[highEnd:lowEnd] {
		low = append(low, stat.Term)
	}
	for _, stat := range termFreqs[start:] {
		universe = append(universe, stat.Term)
	}
	return high, low, universe
}

func buildPrefixStats(postings map[string][]int, cfg WikiTypedConfig) ([]prefixStat, map[string][]string) {
	prefixToTerms := make(map[string]map[string]struct{})
	for term := range postings {
		if len(term) < 4 {
			continue
		}
		maxPrefixLen := min(5, len(term)-1)
		for n := 3; n <= maxPrefixLen; n++ {
			prefix := term[:n]
			if prefixToTerms[prefix] == nil {
				prefixToTerms[prefix] = make(map[string]struct{})
			}
			prefixToTerms[prefix][term] = struct{}{}
		}
	}
	out := make([]prefixStat, 0, len(prefixToTerms))
	for prefix, terms := range prefixToTerms {
		expansion := len(terms)
		if expansion < cfg.PrefixMinExpand || expansion > cfg.PrefixMaxExpand {
			continue
		}
		out = append(out, prefixStat{Prefix: prefix, ExpansionN: expansion})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ExpansionN == out[j].ExpansionN {
			return out[i].Prefix < out[j].Prefix
		}
		return out[i].ExpansionN > out[j].ExpansionN
	})
	prefixTerms := make(map[string][]string, len(out))
	for _, stat := range out {
		termsSet := prefixToTerms[stat.Prefix]
		terms := make([]string, 0, len(termsSet))
		for term := range termsSet {
			terms = append(terms, term)
		}
		sort.Strings(terms)
		prefixTerms[stat.Prefix] = terms
	}
	return out, prefixTerms
}

func buildWikiTypedQrels(stats corpusStats, queries []harness.Query) (quality.Qrels, error) {
	qrels := make(quality.Qrels, len(queries))
	for _, q := range queries {
		docIndexes, err := evalWikiTypedQuery(stats, q)
		if err != nil {
			return nil, err
		}
		relevant := make(map[string]int, len(docIndexes))
		for _, idx := range docIndexes {
			if idx < 0 || idx >= len(stats.docIDs) {
				continue
			}
			relevant[stats.docIDs[idx]] = 1
		}
		qrels[q.ID] = relevant
	}
	return qrels, nil
}

func evalWikiTypedQuery(stats corpusStats, q harness.Query) ([]int, error) {
	switch q.Kind {
	case "", harness.QueryKindText, harness.QueryKindTerm:
		return append([]int(nil), stats.postings[q.Text]...), nil
	case harness.QueryKindPhrase:
		return append([]int(nil), stats.phrases[q.Text]...), nil
	case harness.QueryKindPrefix:
		return prefixDocIndexes(stats, q.Text), nil
	case harness.QueryKindBoolean:
		return evalWikiTypedBoolean(stats, q.Boolean)
	default:
		return nil, fmt.Errorf("dataset: wiki-typed unsupported query kind %q", q.Kind)
	}
}

func evalWikiTypedBoolean(stats corpusStats, spec *harness.BoolQuery) ([]int, error) {
	if spec == nil || len(spec.Clauses) == 0 {
		return nil, nil
	}
	var mustSets, shouldSets [][]int
	var mustNot []int
	for _, clause := range spec.Clauses {
		docs, err := evalWikiTypedAtom(stats, clause.Atom)
		if err != nil {
			return nil, err
		}
		switch clause.Occur {
		case harness.OccurMust:
			mustSets = append(mustSets, docs)
		case harness.OccurShould:
			shouldSets = append(shouldSets, docs)
		case harness.OccurMustNot:
			mustNot = unionDocIndexes(mustNot, docs)
		default:
			return nil, fmt.Errorf("dataset: wiki-typed unsupported bool occur %q", clause.Occur)
		}
	}
	var out []int
	if len(mustSets) > 0 {
		out = append([]int(nil), mustSets[0]...)
		for _, docs := range mustSets[1:] {
			out = intersectDocIndexes(out, docs)
		}
	} else if len(shouldSets) > 0 {
		for _, docs := range shouldSets {
			out = unionDocIndexes(out, docs)
		}
	}
	if len(mustNot) > 0 {
		out = diffDocIndexes(out, mustNot)
	}
	return out, nil
}

func evalWikiTypedAtom(stats corpusStats, atom harness.Atom) ([]int, error) {
	switch atom.Kind {
	case "", harness.QueryKindText, harness.QueryKindTerm:
		return append([]int(nil), stats.postings[atom.Text]...), nil
	case harness.QueryKindPhrase:
		return append([]int(nil), stats.phrases[atom.Text]...), nil
	case harness.QueryKindPrefix:
		return prefixDocIndexes(stats, atom.Text), nil
	default:
		return nil, fmt.Errorf("dataset: wiki-typed unsupported bool atom kind %q", atom.Kind)
	}
}

func prefixDocIndexes(stats corpusStats, prefix string) []int {
	terms := stats.prefixTerms[prefix]
	out := make([]int, 0)
	for _, term := range terms {
		out = unionDocIndexes(out, stats.postings[term])
	}
	return out
}

func unionDocIndexes(left, right []int) []int {
	if len(left) == 0 {
		return append([]int(nil), right...)
	}
	if len(right) == 0 {
		return append([]int(nil), left...)
	}
	out := make([]int, 0, len(left)+len(right))
	i, j := 0, 0
	for i < len(left) && j < len(right) {
		switch {
		case left[i] == right[j]:
			out = append(out, left[i])
			i++
			j++
		case left[i] < right[j]:
			out = append(out, left[i])
			i++
		default:
			out = append(out, right[j])
			j++
		}
	}
	out = append(out, left[i:]...)
	out = append(out, right[j:]...)
	return out
}

func intersectDocIndexes(left, right []int) []int {
	out := make([]int, 0, min(len(left), len(right)))
	i, j := 0, 0
	for i < len(left) && j < len(right) {
		switch {
		case left[i] == right[j]:
			out = append(out, left[i])
			i++
			j++
		case left[i] < right[j]:
			i++
		default:
			j++
		}
	}
	return out
}

func diffDocIndexes(left, right []int) []int {
	if len(left) == 0 {
		return nil
	}
	if len(right) == 0 {
		return append([]int(nil), left...)
	}
	out := make([]int, 0, len(left))
	i, j := 0, 0
	for i < len(left) {
		for j < len(right) && right[j] < left[i] {
			j++
		}
		if j < len(right) && right[j] == left[i] {
			i++
			continue
		}
		out = append(out, left[i])
		i++
	}
	return out
}

func normalizeQueryTypeFilter(values []string) map[string]bool {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]bool, len(values))
	for _, value := range values {
		trimmed := strings.ToLower(strings.TrimSpace(value))
		if trimmed != "" {
			out[trimmed] = true
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func sampleMappedStrings[T any](rng *rand.Rand, values []T, limit int, project func(T) string) []string {
	if len(values) == 0 || limit <= 0 {
		return nil
	}
	perm := rng.Perm(len(values))
	out := make([]string, 0, min(limit, len(values)))
	for _, idx := range perm {
		out = append(out, project(values[idx]))
		if len(out) >= limit {
			break
		}
	}
	return out
}

func sampleStrings(rng *rand.Rand, values []string, limit int) []string {
	if len(values) == 0 || limit <= 0 {
		return nil
	}
	perm := rng.Perm(len(values))
	out := make([]string, 0, min(limit, len(values)))
	for _, idx := range perm {
		out = append(out, values[idx])
		if len(out) >= limit {
			break
		}
	}
	return out
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func orderedPairKey(left, right string) string {
	if left < right {
		return left + "\x00" + right
	}
	return right + "\x00" + left
}

func postingsIntersect(left, right []int) bool {
	i, j := 0, 0
	for i < len(left) && j < len(right) {
		if left[i] == right[j] {
			return true
		}
		if left[i] < right[j] {
			i++
			continue
		}
		j++
	}
	return false
}

func tokenizeWikiText(text string) []string {
	if text == "" {
		return nil
	}
	tokens := make([]string, 0, 32)
	var b strings.Builder
	flush := func() {
		if b.Len() == 0 {
			return
		}
		token := b.String()
		b.Reset()
		if len(token) < 2 {
			return
		}
		tokens = append(tokens, token)
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

func openMaybeCompressed(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	switch ext := strings.ToLower(filepath.Ext(path)); ext {
	case ".gz":
		gz, err := gzip.NewReader(f)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		return &wrappedReadCloser{Reader: gz, closeFn: func() error {
			if err := gz.Close(); err != nil {
				_ = f.Close()
				return err
			}
			return f.Close()
		}}, nil
	case ".bz2":
		return &wrappedReadCloser{Reader: bzip2.NewReader(f), closeFn: f.Close}, nil
	default:
		return f, nil
	}
}

type wrappedReadCloser struct {
	io.Reader
	closeFn func() error
}

func (w *wrappedReadCloser) Close() error { return w.closeFn() }

func wikiTypedQueriesCacheLocation(cfg WikiTypedConfig) (string, string, error) {
	key, err := wikiTypedHashKey(struct {
		Version         string `json:"version"`
		DumpPath        string `json:"dump_path"`
		DumpSize        int64  `json:"dump_size"`
		DumpModUnixNano int64  `json:"dump_mod_unix_nano"`
		MaxDocs         int    `json:"max_docs"`
		Seed            uint64 `json:"seed"`
		QueriesPerClass int    `json:"queries_per_class"`
		HighSkipTop     int    `json:"high_skip_top"`
		HighPool        int    `json:"high_pool"`
		LowPool         int    `json:"low_pool"`
		PrefixMinExpand int    `json:"prefix_min_expand"`
		PrefixMaxExpand int    `json:"prefix_max_expand"`
		IncludeTitle    bool   `json:"include_title_text"`
	}{
		Version:         wikiTypedCacheVersion,
		DumpPath:        mustAbsPath(cfg.DumpPath),
		DumpSize:        mustDumpSize(cfg.DumpPath),
		DumpModUnixNano: mustDumpMod(cfg.DumpPath),
		MaxDocs:         cfg.MaxDocs,
		Seed:            cfg.Seed,
		QueriesPerClass: cfg.QueriesPerClass,
		HighSkipTop:     cfg.HighSkipTop,
		HighPool:        cfg.HighPool,
		LowPool:         cfg.LowPool,
		PrefixMinExpand: cfg.PrefixMinExpand,
		PrefixMaxExpand: cfg.PrefixMaxExpand,
		IncludeTitle:    cfg.IncludeTitleText,
	})
	if err != nil {
		return "", "", err
	}
	return filepath.Join(cfg.CacheDir, key+".queries.json"), key, nil
}

func wikiTypedQrelsCacheLocation(cfg WikiTypedConfig) (string, string, error) {
	queriesPath, key, err := wikiTypedQueriesCacheLocation(cfg)
	if err != nil {
		return "", "", err
	}
	return strings.TrimSuffix(queriesPath, ".queries.json") + ".qrels.json", key, nil
}

func wikiTypedManifestLocation(cfg WikiTypedConfig) (string, error) {
	queriesPath, _, err := wikiTypedQueriesCacheLocation(cfg)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(queriesPath, ".queries.json") + ".manifest.json", nil
}

func wikiTypedDocsCacheLocation(cfg WikiTypedConfig) (string, string, error) {
	absPath, err := filepath.Abs(cfg.DumpPath)
	if err != nil {
		return "", "", fmt.Errorf("dataset: wiki-typed abs dump path: %w", err)
	}
	info, err := os.Stat(cfg.DumpPath)
	if err != nil {
		return "", "", fmt.Errorf("dataset: wiki-typed stat dump: %w", err)
	}
	hashInput := struct {
		Version         string `json:"version"`
		DumpPath        string `json:"dump_path"`
		DumpSize        int64  `json:"dump_size"`
		DumpModUnixNano int64  `json:"dump_mod_unix_nano"`
		MaxDocs         int    `json:"max_docs"`
		IncludeTitle    bool   `json:"include_title_text"`
	}{
		Version:         wikiTypedCacheVersion,
		DumpPath:        absPath,
		DumpSize:        info.Size(),
		DumpModUnixNano: info.ModTime().UnixNano(),
		MaxDocs:         cfg.MaxDocs,
		IncludeTitle:    cfg.IncludeTitleText,
	}
	key, err := wikiTypedHashKey(hashInput)
	if err != nil {
		return "", "", err
	}
	return filepath.Join(cfg.CacheDir, key+".docs.json"), key, nil
}

func loadWikiTypedCachedGroups(queriesPath string, qrelsPath string, key string) ([]QueryGroup, error) {
	data, err := os.ReadFile(queriesPath)
	if err != nil {
		return nil, err
	}
	var queriesCache wikiTypedQueriesCacheFile
	if err := json.Unmarshal(data, &queriesCache); err != nil {
		return nil, fmt.Errorf("dataset: wiki-typed unmarshal queries cache: %w", err)
	}
	if queriesCache.Version != wikiTypedCacheVersion {
		return nil, fmt.Errorf("dataset: wiki-typed queries cache version mismatch")
	}
	if queriesCache.Key != key {
		return nil, fmt.Errorf("dataset: wiki-typed queries cache key mismatch")
	}
	if len(queriesCache.Groups) == 0 {
		return nil, fmt.Errorf("dataset: wiki-typed queries cache has no groups")
	}

	qrelsData, err := os.ReadFile(qrelsPath)
	if err != nil {
		return nil, err
	}
	var qrelsCache wikiTypedQrelsCacheFile
	if err := json.Unmarshal(qrelsData, &qrelsCache); err != nil {
		return nil, fmt.Errorf("dataset: wiki-typed unmarshal qrels cache: %w", err)
	}
	if qrelsCache.Version != wikiTypedCacheVersion {
		return nil, fmt.Errorf("dataset: wiki-typed qrels cache version mismatch")
	}
	if qrelsCache.Key != key {
		return nil, fmt.Errorf("dataset: wiki-typed qrels cache key mismatch")
	}
	if len(qrelsCache.Groups) == 0 {
		return nil, fmt.Errorf("dataset: wiki-typed qrels cache has no groups")
	}

	qrelsByName := make(map[string]quality.Qrels, len(qrelsCache.Groups))
	for _, group := range qrelsCache.Groups {
		if len(group.Qrels) == 0 {
			return nil, fmt.Errorf("dataset: wiki-typed qrels cache missing qrels for group %q", group.Name)
		}
		qrelsByName[group.Name] = group.Qrels
	}

	groups := make([]QueryGroup, 0, len(queriesCache.Groups))
	for _, group := range queriesCache.Groups {
		qrels, ok := qrelsByName[group.Name]
		if !ok {
			return nil, fmt.Errorf("dataset: wiki-typed qrels cache missing group %q", group.Name)
		}
		groups = append(groups, QueryGroup{Name: group.Name, Queries: group.Queries, Qrels: qrels})
	}
	return groups, nil
}

func saveWikiTypedCachedGroups(queriesPath string, qrelsPath string, key string, groups []QueryGroup) error {
	if err := os.MkdirAll(filepath.Dir(queriesPath), 0o755); err != nil {
		return fmt.Errorf("dataset: wiki-typed create queries/qrels cache dir: %w", err)
	}
	queriesGroups := make([]cachedQueryGroup, 0, len(groups))
	qrelsGroups := make([]cachedQrelsGroup, 0, len(groups))
	for _, group := range groups {
		queriesGroups = append(queriesGroups, cachedQueryGroup{Name: group.Name, Queries: group.Queries})
		qrelsGroups = append(qrelsGroups, cachedQrelsGroup{Name: group.Name, Qrels: group.Qrels})
	}
	queriesCache := wikiTypedQueriesCacheFile{Version: wikiTypedCacheVersion, Key: key, Groups: queriesGroups}
	queriesData, err := json.MarshalIndent(queriesCache, "", "  ")
	if err != nil {
		return fmt.Errorf("dataset: wiki-typed marshal queries cache: %w", err)
	}
	if err := os.WriteFile(queriesPath, queriesData, 0o644); err != nil {
		return fmt.Errorf("dataset: wiki-typed write queries cache: %w", err)
	}
	qrelsCache := wikiTypedQrelsCacheFile{Version: wikiTypedCacheVersion, Key: key, Groups: qrelsGroups}
	qrelsData, err := json.MarshalIndent(qrelsCache, "", "  ")
	if err != nil {
		return fmt.Errorf("dataset: wiki-typed marshal qrels cache: %w", err)
	}
	if err := os.WriteFile(qrelsPath, qrelsData, 0o644); err != nil {
		return fmt.Errorf("dataset: wiki-typed write qrels cache: %w", err)
	}
	return nil
}

func loadWikiTypedCachedDocs(path string, key string) ([]harness.Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cache wikiTypedDocsCacheFile
	if err := json.Unmarshal(data, &cache); err != nil {
		return nil, fmt.Errorf("dataset: wiki-typed unmarshal docs cache: %w", err)
	}
	if cache.Version != wikiTypedCacheVersion {
		return nil, fmt.Errorf("dataset: wiki-typed docs cache version mismatch")
	}
	if cache.Key != key {
		return nil, fmt.Errorf("dataset: wiki-typed docs cache key mismatch")
	}
	if len(cache.Docs) == 0 {
		return nil, fmt.Errorf("dataset: wiki-typed docs cache is empty")
	}
	return cache.Docs, nil
}

func saveWikiTypedCachedDocs(path string, key string, docs []harness.Document) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("dataset: wiki-typed create docs cache dir: %w", err)
	}
	cache := wikiTypedDocsCacheFile{Version: wikiTypedCacheVersion, Key: key, Docs: docs}
	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return fmt.Errorf("dataset: wiki-typed marshal docs cache: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("dataset: wiki-typed write docs cache: %w", err)
	}
	return nil
}

func saveWikiTypedManifest(path string, docs docsCacheResult, queriesPath string, qrelsPath string, queryKey string, cfg WikiTypedConfig) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("dataset: wiki-typed create manifest dir: %w", err)
	}
	manifest := wikiTypedManifestFile{
		Version:     wikiTypedCacheVersion,
		DumpPath:    mustAbsPath(cfg.DumpPath),
		CacheDir:    cfg.CacheDir,
		DocsKey:     docs.cacheKey,
		QueryKey:    queryKey,
		DocsFile:    docs.cachePath,
		QueriesFile: queriesPath,
		QrelsFile:   qrelsPath,
		Params: map[string]any{
			"max_docs":           cfg.MaxDocs,
			"seed":               cfg.Seed,
			"queries_per_class":  cfg.QueriesPerClass,
			"high_skip_top":      cfg.HighSkipTop,
			"high_pool":          cfg.HighPool,
			"low_pool":           cfg.LowPool,
			"prefix_min_expand":  cfg.PrefixMinExpand,
			"prefix_max_expand":  cfg.PrefixMaxExpand,
			"include_title_text": cfg.IncludeTitleText,
			"query_types":        append([]string(nil), cfg.QueryTypes...),
			"k":                  cfg.K,
		},
		Files: map[string]cacheFileRef{
			"docs":    {Path: docs.cachePath, Kind: "docs"},
			"queries": {Path: queriesPath, Kind: "queries"},
			"qrels":   {Path: qrelsPath, Kind: "qrels"},
		},
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("dataset: wiki-typed marshal manifest: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("dataset: wiki-typed write manifest: %w", err)
	}
	return nil
}

func wikiTypedHashKey(input any) (string, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("dataset: wiki-typed marshal cache fingerprint: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func mustAbsPath(path string) string {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return absPath
}

func mustDumpSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func mustDumpMod(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.ModTime().UnixNano()
}
