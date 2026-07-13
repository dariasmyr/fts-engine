package fts

import (
	"context"
	"sort"
	"strings"
	"testing"
)

type prefixMemoryIndex struct {
	entries   map[string][]DocRef
	positions map[string]map[DocOrd][]uint32
}

type fixedScorer float64

func (s fixedScorer) Score(TermStats, DocStats, FieldStats) float64 { return float64(s) }

type tfScorer struct{}

func (tfScorer) Score(t TermStats, _ DocStats, _ FieldStats) float64 { return float64(t.TF) }

func newPrefixMemoryIndex() *prefixMemoryIndex {
	return &prefixMemoryIndex{
		entries:   make(map[string][]DocRef),
		positions: make(map[string]map[DocOrd][]uint32),
	}
}

func (p *prefixMemoryIndex) Insert(key string, ord DocOrd) error {
	return p.insert(key, ord, 0, false)
}

func (p *prefixMemoryIndex) InsertAt(key string, position uint32, ord DocOrd) error {
	return p.insert(key, ord, position, true)
}

func (p *prefixMemoryIndex) insert(key string, ord DocOrd, position uint32, hasPosition bool) error {
	entries := p.entries[key]
	for i := range entries {
		if entries[i].Ord == ord {
			entries[i].Count++
			p.entries[key] = entries
			if hasPosition {
				p.addPosition(key, ord, position)
			}
			return nil
		}
	}
	p.entries[key] = append(entries, DocRef{Ord: ord, Count: 1, Seq: uint32(ord)})
	if hasPosition {
		p.addPosition(key, ord, position)
	}
	return nil
}

func (p *prefixMemoryIndex) addPosition(key string, ord DocOrd, position uint32) {
	perKey := p.positions[key]
	if perKey == nil {
		perKey = make(map[DocOrd][]uint32)
		p.positions[key] = perKey
	}
	perKey[ord] = append(perKey[ord], position)
}

func (p *prefixMemoryIndex) Search(key string) ([]DocRef, error) {
	return p.entries[key], nil
}

func (p *prefixMemoryIndex) SearchPositional(key string) ([]PositionalDocRef, error) {
	docs := p.entries[key]
	out := make([]PositionalDocRef, 0, len(docs))
	perKey := p.positions[key]
	for _, doc := range docs {
		out = append(out, PositionalDocRef{Ord: doc.Ord, Positions: perKey[doc.Ord]})
	}
	return out, nil
}

func (p *prefixMemoryIndex) SearchPrefix(prefix string) ([]DocRef, error) {
	merged := make(map[DocOrd]uint32)
	for key, docs := range p.entries {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, doc := range docs {
			merged[doc.Ord] += doc.Count
		}
	}

	out := make([]DocRef, 0, len(merged))
	for ord, count := range merged {
		out = append(out, DocRef{Ord: ord, Count: count, Seq: uint32(ord)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ord < out[j].Ord })
	return out, nil
}

func TestBM25RareTermScoresHigherThanCommon(t *testing.T) {
	scorer := BM25()

	fieldStats := FieldStats{N: 1000, AvgLength: 20}
	doc := DocStats{Ord: 1, Length: 20}

	rare := scorer.Score(TermStats{Term: "rosa", TF: 1, DF: 3}, doc, fieldStats)
	common := scorer.Score(TermStats{Term: "the", TF: 1, DF: 900}, doc, fieldStats)

	if rare <= common {
		t.Fatalf("expected rare term score > common term score, got rare=%v common=%v", rare, common)
	}
}

func TestBM25LengthNormalization(t *testing.T) {
	scorer := BM25()
	fieldStats := FieldStats{N: 100, AvgLength: 50}
	term := TermStats{Term: "x", TF: 2, DF: 10}

	short := scorer.Score(term, DocStats{Ord: 1, Length: 10}, fieldStats)
	long := scorer.Score(term, DocStats{Ord: 2, Length: 200}, fieldStats)

	if short <= long {
		t.Fatalf("expected shorter document to score higher, got short=%v long=%v", short, long)
	}
}

func TestTFIDFMonotonicInTF(t *testing.T) {
	scorer := TFIDF()
	fieldStats := FieldStats{N: 100, AvgLength: 10}

	low := scorer.Score(TermStats{Term: "alpha", TF: 1, DF: 5}, DocStats{Ord: 1, Length: 10}, fieldStats)
	high := scorer.Score(TermStats{Term: "alpha", TF: 10, DF: 5}, DocStats{Ord: 1, Length: 10}, fieldStats)

	if high <= low {
		t.Fatalf("expected TF-IDF to increase with TF, got low=%v high=%v", low, high)
	}
}

func TestWeightedScorerUsesDefaultWeight(t *testing.T) {
	scorer := WeightedScorer{Base: fixedScorer(2), DefaultWeight: 4}

	got := scorer.Score(TermStats{Field: "body"}, DocStats{}, FieldStats{})
	if got != 8 {
		t.Fatalf("expected weighted score 8, got %v", got)
	}
}

func TestWeightedScorerDefaultsToOne(t *testing.T) {
	scorer := WeightedScorer{Base: fixedScorer(2)}

	got := scorer.Score(TermStats{Field: "body"}, DocStats{}, FieldStats{})
	if got != 2 {
		t.Fatalf("expected default weight 1, got score %v", got)
	}
}

func TestWeightedScorerFieldWeightOverridesDefault(t *testing.T) {
	scorer := WeightedScorer{
		Base:          fixedScorer(2),
		DefaultWeight: 4,
		FieldWeights:  map[string]float64{"title": 3},
	}

	got := scorer.Score(TermStats{Field: "title"}, DocStats{}, FieldStats{})
	if got != 6 {
		t.Fatalf("expected field weight to override default, got %v", got)
	}
}

func TestWeightedScorerAppliesMatchWeight(t *testing.T) {
	scorer := WeightedScorer{
		Base:         fixedScorer(2),
		MatchWeights: MatchWeights{Phrase: 4},
	}

	got := scorer.Score(TermStats{MatchType: MatchPhrase}, DocStats{}, FieldStats{})
	if got != 8 {
		t.Fatalf("expected phrase-weighted score 8, got %v", got)
	}
}

func TestWeightedScorerAllowsZeroFieldWeight(t *testing.T) {
	scorer := WeightedScorer{
		Base:         fixedScorer(2),
		FieldWeights: map[string]float64{"hidden": 0},
	}

	got := scorer.Score(TermStats{Field: "hidden"}, DocStats{}, FieldStats{})
	if got != 0 {
		t.Fatalf("expected zero field weight to suppress score, got %v", got)
	}
}

func TestWeightedScorerNilBaseReturnsZero(t *testing.T) {
	scorer := WeightedScorer{FieldWeights: map[string]float64{"title": 3}}

	got := scorer.Score(TermStats{Field: "title"}, DocStats{}, FieldStats{})
	if got != 0 {
		t.Fatalf("expected zero score without base scorer, got %v", got)
	}
}

func TestSearchWithBM25RanksRareDocumentFirst(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys, WithScorer(BM25()))

	ctx := context.Background()
	docs := map[string]string{
		"doc-a": "rosa barge",
		"doc-b": "barge barge barge",
	}
	for _, id := range []string{"doc-c", "doc-d", "doc-e", "doc-f", "doc-g", "doc-h", "doc-i", "doc-j"} {
		docs[id] = "barge"
	}
	for id, content := range docs {
		if err := indexDefaultDoc(ctx, svc, DocID(id), content); err != nil {
			t.Fatalf("Index(%q) error = %v", id, err)
		}
	}

	res, err := svc.SearchDocuments(ctx, "rosa barge", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) < 2 {
		t.Fatalf("expected at least 2 results, got %+v", res.Results)
	}
	if res.Results[0].ID != "doc-a" {
		t.Fatalf("expected doc-a to rank first, got %+v", res.Results)
	}
	if res.Results[0].Score <= res.Results[1].Score {
		t.Fatalf("expected scores to be non-increasing, got %v then %v", res.Results[0].Score, res.Results[1].Score)
	}
}

func TestSearchWithTFIDFRanksRareDocumentFirst(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys, WithScorer(TFIDF()))

	ctx := context.Background()
	docs := map[string]string{
		"doc-a": "rosa barge",
		"doc-b": "barge barge barge",
	}
	for _, id := range []string{"doc-c", "doc-d", "doc-e", "doc-f", "doc-g", "doc-h", "doc-i", "doc-j"} {
		docs[id] = "barge"
	}
	for id, content := range docs {
		if err := indexDefaultDoc(ctx, svc, DocID(id), content); err != nil {
			t.Fatalf("Index(%q) error = %v", id, err)
		}
	}

	res, err := svc.SearchDocuments(ctx, "rosa barge", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) < 2 {
		t.Fatalf("expected at least 2 results, got %+v", res.Results)
	}
	if res.Results[0].ID != "doc-a" {
		t.Fatalf("expected doc-a to rank first, got %+v", res.Results)
	}
	if res.Results[0].Score <= res.Results[1].Score {
		t.Fatalf("expected scores to be non-increasing, got %v then %v", res.Results[0].Score, res.Results[1].Score)
	}
}

func TestSearchWithWeightedScorerRanksHigherWeightedFieldFirst(t *testing.T) {
	factory := func(string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys, WithScorer(WeightedScorer{
		Base: tfScorer{},
		FieldWeights: map[string]float64{
			"title": 4,
			"body":  1,
		},
	}))

	ctx := context.Background()
	docs := []Document{
		{ID: "doc-a", Fields: map[string]Field{"title": {Value: "alpha"}}},
		{ID: "doc-b", Fields: map[string]Field{"body": {Value: "alpha alpha alpha"}}},
	}
	for _, doc := range docs {
		if err := svc.Index(ctx, doc); err != nil {
			t.Fatalf("Index(%q) error = %v", doc.ID, err)
		}
	}

	res, err := svc.SearchDocuments(ctx, "alpha", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) != 2 {
		t.Fatalf("expected 2 results, got %+v", res.Results)
	}
	if res.Results[0].ID != "doc-a" {
		t.Fatalf("expected title-weighted doc-a first, got %+v", res.Results)
	}
	if res.Results[0].Score != 4 || res.Results[1].Score != 3 {
		t.Fatalf("expected weighted scores 4 and 3, got %+v", res.Results)
	}
}

func TestSearchWithRankProfileUsesFieldWeights(t *testing.T) {
	factory := func(string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys, WithRankProfile(RankProfile{
		Name: "docs",
		Base: tfScorer{},
		FieldWeights: map[string]float64{
			"title": 5,
			"body":  1,
		},
	}))

	ctx := context.Background()
	docs := []Document{
		{ID: "doc-a", Fields: map[string]Field{"title": {Value: "alpha"}}},
		{ID: "doc-b", Fields: map[string]Field{"body": {Value: "alpha alpha alpha alpha"}}},
	}
	for _, doc := range docs {
		if err := svc.Index(ctx, doc); err != nil {
			t.Fatalf("Index(%q) error = %v", doc.ID, err)
		}
	}

	res, err := svc.SearchDocuments(ctx, "alpha", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) != 2 {
		t.Fatalf("expected 2 results, got %+v", res.Results)
	}
	if res.Results[0].ID != "doc-a" {
		t.Fatalf("expected rank profile to put doc-a first, got %+v", res.Results)
	}
	if res.Results[0].Score != 5 || res.Results[1].Score != 4 {
		t.Fatalf("expected profile scores 5 and 4, got %+v", res.Results)
	}
}

func TestSearchWithRankProfileUsesMatchWeights(t *testing.T) {
	factory := func(string) (Index, error) { return newPrefixMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys, WithRankProfile(RankProfile{
		Name: "docs",
		Base: tfScorer{},
		MatchWeights: MatchWeights{
			Prefix: 0.5,
			Phrase: 4,
		},
	}))

	ctx := context.Background()
	docs := []Document{
		{ID: "doc-phrase", Fields: map[string]Field{"body": {Value: "alpha beta"}}},
		{ID: "doc-prefix", Fields: map[string]Field{"body": {Value: "alphabet alphabet alphabet alphabet alphabet"}}},
	}
	for _, doc := range docs {
		if err := svc.Index(ctx, doc); err != nil {
			t.Fatalf("Index(%q) error = %v", doc.ID, err)
		}
	}

	res, err := svc.Search(ctx, &BooleanQuery{Clauses: []BoolClause{
		ShouldClause(PhraseQuery{Phrase: "alpha beta"}),
		ShouldClause(PrefixQuery{Prefix: "alph"}),
	}}, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(res.Results) != 2 {
		t.Fatalf("expected 2 results, got %+v", res.Results)
	}
	if res.Results[0].ID != "doc-phrase" {
		t.Fatalf("expected phrase-weighted doc first, got %+v", res.Results)
	}
	if res.Results[0].Score != 4.5 || res.Results[1].Score != 2.5 {
		t.Fatalf("expected match-weighted scores 4.5 and 2.5, got %+v", res.Results)
	}
}

func TestExplainWithRankProfileShowsWeightedContribution(t *testing.T) {
	factory := func(string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys, WithRankProfile(RankProfile{
		Name: "docs",
		Base: tfScorer{},
		FieldWeights: map[string]float64{
			"title": 5,
			"body":  1,
		},
	}))

	ctx := context.Background()
	docs := []Document{
		{ID: "doc-a", Fields: map[string]Field{"title": {Value: "alpha"}}},
		{ID: "doc-b", Fields: map[string]Field{"body": {Value: "alpha alpha alpha alpha"}}},
	}
	for _, doc := range docs {
		if err := svc.Index(ctx, doc); err != nil {
			t.Fatalf("Index(%q) error = %v", doc.ID, err)
		}
	}

	explanation, err := svc.Explain(ctx, "alpha", "doc-a")
	if err != nil {
		t.Fatalf("Explain() error = %v", err)
	}
	if !explanation.Matched {
		t.Fatalf("expected doc-a to match, got %+v", explanation)
	}
	if explanation.Score != 5 || explanation.UniqueMatches != 1 || explanation.TotalMatches != 1 {
		t.Fatalf("unexpected explanation summary: %+v", explanation)
	}
	if len(explanation.Contributions) != 1 {
		t.Fatalf("expected 1 contribution, got %+v", explanation.Contributions)
	}
	c := explanation.Contributions[0]
	if c.Field != "title" || c.Term != "alpha" {
		t.Fatalf("unexpected contribution identity: %+v", c)
	}
	if c.BaseScore != 1 || c.FieldWeight != 5 || c.Score != 5 {
		t.Fatalf("unexpected weighted contribution: %+v", c)
	}
	if c.MatchType != MatchTerm || c.MatchWeight != 1 {
		t.Fatalf("unexpected match contribution: %+v", c)
	}
	if c.TF != 1 || c.DF != 1 || c.DocLength != 1 || c.FieldDocs != 1 {
		t.Fatalf("unexpected contribution stats: %+v", c)
	}
}

func TestExplainWithRankProfileShowsMatchWeight(t *testing.T) {
	factory := func(string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys, WithRankProfile(RankProfile{
		Name:         "docs",
		Base:         tfScorer{},
		MatchWeights: MatchWeights{Phrase: 4},
	}))

	ctx := context.Background()
	if err := svc.Index(ctx, Document{ID: "doc-a", Fields: map[string]Field{"body": {Value: "alpha beta"}}}); err != nil {
		t.Fatalf("Index(doc-a) error = %v", err)
	}

	explanation, err := svc.Explain(ctx, `"alpha beta"`, "doc-a")
	if err != nil {
		t.Fatalf("Explain() error = %v", err)
	}
	if !explanation.Matched || explanation.Score != 4 {
		t.Fatalf("unexpected explanation summary: %+v", explanation)
	}
	if len(explanation.Contributions) != 1 {
		t.Fatalf("expected 1 contribution, got %+v", explanation.Contributions)
	}
	c := explanation.Contributions[0]
	if c.MatchType != MatchPhrase || c.MatchWeight != 4 || c.BaseScore != 1 || c.Score != 4 {
		t.Fatalf("unexpected phrase contribution: %+v", c)
	}
}

func TestExplainUnknownDocumentReturnsUnmatched(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys, WithScorer(tfScorer{}))

	explanation, err := svc.Explain(context.Background(), "alpha", "missing")
	if err != nil {
		t.Fatalf("Explain() error = %v", err)
	}
	if explanation.ID != "missing" || explanation.Matched || explanation.Score != 0 || len(explanation.Contributions) != 0 {
		t.Fatalf("unexpected explanation for missing doc: %+v", explanation)
	}
}

func TestBooleanScoringAppliesShouldBoost(t *testing.T) {
	idx := newMemoryIndex()

	svc := New(idx, WordKeys, WithScorer(TFIDF()))
	idx.entries["alpha"] = refsForIDs(svc.registry, namedPosting{"doc-a", 1}, namedPosting{"doc-b", 1}, namedPosting{"doc-c", 1})
	idx.entries["beta"] = refsForIDs(svc.registry, namedPosting{"doc-a", 1}, namedPosting{"doc-b", 1})
	idx.entries["delta"] = refsForIDs(svc.registry, namedPosting{"doc-b", 1}, namedPosting{"doc-d", 1})
	for _, doc := range []struct {
		id     DocID
		length uint32
	}{
		{id: "doc-a", length: 2},
		{id: "doc-b", length: 3},
		{id: "doc-c", length: 1},
		{id: "doc-d", length: 1},
	} {
		svc.collection.observe(DefaultField, svc.registry.GetOrAssign(doc.id), doc.length)
	}

	q := &BooleanQuery{Clauses: []BoolClause{
		MustClause(TermQuery{Term: "alpha"}),
		MustClause(TermQuery{Term: "beta"}),
		ShouldClause(TermQuery{Term: "delta"}),
	}}

	res, err := svc.Search(context.Background(), q, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(res.Results) != 2 {
		t.Fatalf("expected 2 results, got %+v", res.Results)
	}
	if res.Results[0].ID != "doc-b" {
		t.Fatalf("expected doc-b to rank first after SHOULD boost, got %+v", res.Results)
	}
	if res.Results[0].Score <= res.Results[1].Score {
		t.Fatalf("expected boosted score to be higher, got %+v", res.Results)
	}
}

func TestSearchPhraseWithTFIDFScoresAcrossFields(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys, WithScorer(TFIDF()))

	ctx := context.Background()
	docs := []Document{
		{ID: "doc-a", Fields: map[string]Field{"title": {Value: "james doe"}}},
		{ID: "doc-b", Fields: map[string]Field{"body": {Value: "james doe james doe"}}},
		{ID: "doc-c", Fields: map[string]Field{"title": {Value: "speech only"}}},
		{ID: "doc-d", Fields: map[string]Field{"body": {Value: "speech only"}}},
	}
	for _, doc := range docs {
		if err := svc.Index(ctx, doc); err != nil {
			t.Fatalf("Index(%q) error = %v", doc.ID, err)
		}
	}

	res, err := svc.SearchDocuments(ctx, `"james doe"`, 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) != 2 {
		t.Fatalf("expected 2 phrase hits, got %+v", res.Results)
	}
	if res.Results[0].ID != "doc-b" {
		t.Fatalf("expected doc-b to rank first by phrase TF, got %+v", res.Results)
	}
	if res.Results[0].Score <= res.Results[1].Score {
		t.Fatalf("expected scores to be non-increasing, got %+v", res.Results)
	}
}

func TestSearchWithPrefixScoringRanksHigherFrequencyFirst(t *testing.T) {
	svc := New(newPrefixMemoryIndex(), WordKeys, WithScorer(TFIDF()))

	ctx := context.Background()
	if err := indexDefaultDoc(ctx, svc, "doc-a", "bar barge"); err != nil {
		t.Fatalf("Index(doc-a) error = %v", err)
	}
	if err := indexDefaultDoc(ctx, svc, "doc-b", "bar"); err != nil {
		t.Fatalf("Index(doc-b) error = %v", err)
	}
	if err := indexDefaultDoc(ctx, svc, "doc-c", "hotel"); err != nil {
		t.Fatalf("Index(doc-c) error = %v", err)
	}

	res, err := svc.SearchDocuments(ctx, "bar*", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) != 2 {
		t.Fatalf("expected 2 prefix hits, got %+v", res.Results)
	}
	if res.Results[0].ID != "doc-a" {
		t.Fatalf("expected doc-a to rank first by prefix TF, got %+v", res.Results)
	}
	if res.Results[0].Score <= res.Results[1].Score {
		t.Fatalf("expected scores to be non-increasing, got %+v", res.Results)
	}
}

func TestSearchScoreStaysZeroWithoutScorer(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys)

	ctx := context.Background()
	if err := indexDefaultDoc(ctx, svc, "doc-a", "alpha beta"); err != nil {
		t.Fatalf("Index(doc-a) error = %v", err)
	}

	res, err := svc.SearchDocuments(ctx, "alpha", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) != 1 {
		t.Fatalf("expected 1 result, got %+v", res.Results)
	}
	if res.Results[0].Score != 0 {
		t.Fatalf("expected zero score without scorer, got %v", res.Results[0].Score)
	}
}
