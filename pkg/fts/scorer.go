package fts

import "math"

type TermStats struct {
	Field string
	Term  string
	TF    uint32 // term frequency: matches in this document
	DF    uint32 // document frequency: documents containing the term in this field
}

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

func (s *Service) scoreTermHit(field string, term string, ord DocOrd, tf uint32, df uint32, stats FieldStats) float64 {
	if s.scorer == nil || s.collection == nil {
		return 0
	}
	ts := TermStats{Field: field, Term: term, TF: tf, DF: df}
	ds := DocStats{Ord: ord, Length: s.collection.DocLen(field, ord)}
	return s.scorer.Score(ts, ds, stats)
}

func (s *Service) scoreTermExpansionDoc(exp termExpansion, doc DocRef) float64 {
	return s.scoreTermHit(exp.field, exp.term, doc.Ord, doc.Count, exp.df, exp.fieldStats)
}

func (s *Service) scoreTermExpansionTF(exp termExpansion, ord DocOrd, tf uint32) float64 {
	return s.scoreTermHit(exp.field, exp.term, ord, tf, exp.df, exp.fieldStats)
}
