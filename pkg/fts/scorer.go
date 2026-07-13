package fts

import (
	"context"
	"math"
)

type TermStats struct {
	Field     string
	Term      string
	MatchType MatchType
	TF        uint32 // term frequency: matches in this document
	DF        uint32 // document frequency: documents containing the term in this field
}

type MatchType string

const (
	MatchTerm       MatchType = "term"
	MatchPrefix     MatchType = "prefix"
	MatchPhrase     MatchType = "phrase"
	MatchNearPhrase MatchType = "near_phrase"
)

type DocStats struct {
	Ord    DocOrd
	Length uint32
}

type FieldStats struct {
	N         int
	AvgLength float64
}

type Scorer interface {
	Score(TermStats, DocStats, FieldStats) float64
}

type RankProfile struct {
	Name         string
	Base         Scorer
	FieldWeights map[string]float64
	MatchWeights MatchWeights
}

type WeightedScorer struct {
	Base          Scorer
	FieldWeights  map[string]float64
	MatchWeights  MatchWeights
	DefaultWeight float64
}

type MatchWeights struct {
	Term       float64
	Prefix     float64
	Phrase     float64
	NearPhrase float64
}

func (s WeightedScorer) Score(t TermStats, d DocStats, f FieldStats) float64 {
	if s.Base == nil {
		return 0
	}
	return s.Base.Score(t, d, f) * s.fieldWeight(t.Field) * s.matchWeight(t.MatchType)
}

func (s WeightedScorer) fieldWeight(field string) float64 {
	weight := s.DefaultWeight
	if weight == 0 {
		weight = 1
	}
	if fieldWeight, ok := s.FieldWeights[field]; ok {
		weight = fieldWeight
	}
	return weight
}

func (s WeightedScorer) matchWeight(matchType MatchType) float64 {
	return s.MatchWeights.weight(matchType)
}

func (w MatchWeights) weight(matchType MatchType) float64 {
	switch matchType {
	case MatchPrefix:
		if w.Prefix != 0 {
			return w.Prefix
		}
	case MatchPhrase:
		if w.Phrase != 0 {
			return w.Phrase
		}
	case MatchNearPhrase:
		if w.NearPhrase != 0 {
			return w.NearPhrase
		}
	default:
		if w.Term != 0 {
			return w.Term
		}
	}
	return 1
}

type BM25Scorer struct {
	K1 float64
	B  float64
}

func BM25() *BM25Scorer {
	return &BM25Scorer{K1: 1.2, B: 0.75}
}

func (s *BM25Scorer) Score(t TermStats, d DocStats, f FieldStats) float64 {
	if t.DF == 0 || f.N == 0 || t.TF == 0 {
		return 0
	}

	k1, b := s.K1, s.B
	if k1 <= 0 {
		k1 = 1.2
	}
	if b < 0 || b > 1 {
		b = 0.75
	}

	idf := math.Log(float64(f.N)-float64(t.DF)+0.5) - math.Log(float64(t.DF)+0.5)
	idf = math.Log1p(math.Exp(idf))

	norm := 1.0
	if f.AvgLength > 0 {
		norm = 1 - b + b*float64(d.Length)/f.AvgLength
	}

	tf := float64(t.TF)
	return idf * (tf * (k1 + 1)) / (tf + k1*norm)
}

type TFIDFScorer struct{}

func TFIDF() *TFIDFScorer { return &TFIDFScorer{} }

func (TFIDFScorer) Score(t TermStats, d DocStats, f FieldStats) float64 {
	if t.DF == 0 || f.N == 0 || t.TF == 0 {
		return 0
	}
	idf := math.Log(float64(f.N) / float64(t.DF))
	if idf < 0 {
		idf = 0
	}
	return float64(t.TF) * idf
}

func (s *Service) fieldStatsFor(field string) FieldStats {
	if s.scorer == nil || s.collection == nil {
		return FieldStats{}
	}
	return FieldStats{
		N:         s.collection.FieldDocCount(field),
		AvgLength: s.collection.AvgDocLen(field),
	}
}

func (s *Service) scoreTermHit(ctx context.Context, field string, term string, matchType MatchType, ord DocOrd, tf uint32, df uint32, stats FieldStats) float64 {
	if s.scorer == nil || s.collection == nil {
		return 0
	}
	if matchType == "" {
		matchType = MatchTerm
	}
	ts := TermStats{Field: field, Term: term, MatchType: matchType, TF: tf, DF: df}
	ds := DocStats{Ord: ord, Length: s.collection.DocLen(field, ord)}
	baseScore, fieldWeight, matchWeight, score := scoreWithBreakdown(s.scorer, ts, ds, stats)
	if exp := explanationFromContext(ctx); exp != nil {
		exp.add(ord, ScoreContribution{
			Field:          field,
			Term:           term,
			MatchType:      matchType,
			TF:             tf,
			DF:             df,
			DocLength:      ds.Length,
			FieldDocs:      stats.N,
			AvgFieldLength: stats.AvgLength,
			BaseScore:      baseScore,
			FieldWeight:    fieldWeight,
			MatchWeight:    matchWeight,
			Score:          score,
		})
	}
	return score
}

func (s *Service) scoreTermExpansionDoc(ctx context.Context, exp termExpansion, doc DocRef) float64 {
	return s.scoreTermExpansionDocType(ctx, exp, doc, MatchTerm)
}

func (s *Service) scoreTermExpansionDocType(ctx context.Context, exp termExpansion, doc DocRef, matchType MatchType) float64 {
	return s.scoreTermHit(ctx, exp.field, exp.term, matchType, doc.Ord, doc.Count, exp.df, exp.fieldStats)
}

func (s *Service) scoreTermExpansionTF(ctx context.Context, exp termExpansion, ord DocOrd, tf uint32) float64 {
	return s.scoreTermExpansionTFType(ctx, exp, ord, tf, MatchTerm)
}

func (s *Service) scoreTermExpansionTFType(ctx context.Context, exp termExpansion, ord DocOrd, tf uint32, matchType MatchType) float64 {
	return s.scoreTermHit(ctx, exp.field, exp.term, matchType, ord, tf, exp.df, exp.fieldStats)
}

func scoreWithBreakdown(scorer Scorer, t TermStats, d DocStats, f FieldStats) (baseScore float64, fieldWeight float64, matchWeight float64, score float64) {
	fieldWeight = 1
	matchWeight = 1
	switch scorer := scorer.(type) {
	case WeightedScorer:
		if scorer.Base == nil {
			return 0, scorer.fieldWeight(t.Field), scorer.matchWeight(t.MatchType), 0
		}
		baseScore = scorer.Base.Score(t, d, f)
		fieldWeight = scorer.fieldWeight(t.Field)
		matchWeight = scorer.matchWeight(t.MatchType)
		return baseScore, fieldWeight, matchWeight, baseScore * fieldWeight * matchWeight
	case *WeightedScorer:
		if scorer == nil || scorer.Base == nil {
			return 0, 1, 1, 0
		}
		baseScore = scorer.Base.Score(t, d, f)
		fieldWeight = scorer.fieldWeight(t.Field)
		matchWeight = scorer.matchWeight(t.MatchType)
		return baseScore, fieldWeight, matchWeight, baseScore * fieldWeight * matchWeight
	default:
		score = scorer.Score(t, d, f)
		return score, 1, 1, score
	}
}
