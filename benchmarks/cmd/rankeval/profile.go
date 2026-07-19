package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"strings"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

type rankProfileFile struct {
	Name         string             `json:"name"`
	Base         string             `json:"base"`
	FieldWeights map[string]float64 `json:"field_weights"`
}

func loadRankProfile(path string) (*rankProfileFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read profile %q: %w", path, err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var profile rankProfileFile
	if err := dec.Decode(&profile); err != nil {
		return nil, fmt.Errorf("decode profile %q: %w", path, err)
	}
	if err := profile.validate(); err != nil {
		return nil, fmt.Errorf("profile %q: %w", path, err)
	}
	profile.Base = normalizedScorerName(profile.Base)
	profile.FieldWeights = copyFieldWeights(profile.FieldWeights)
	return &profile, nil
}

func (p rankProfileFile) validate() error {
	if p.Name == "" {
		return errors.New("name is required")
	}
	if _, err := scorerByName(p.Base); err != nil {
		return err
	}
	if len(p.FieldWeights) == 0 {
		return errors.New("field_weights must not be empty")
	}
	for field, weight := range p.FieldWeights {
		if field == "" {
			return errors.New("field_weights contains an empty field name")
		}
		if weight < 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
			return fmt.Errorf("field %q has invalid weight %v", field, weight)
		}
	}
	return nil
}

func scorerByName(name string) (fts.Scorer, error) {
	switch normalizedScorerName(name) {
	case "bm25":
		return fts.BM25(), nil
	case "tfidf":
		return fts.TFIDF(), nil
	default:
		return nil, fmt.Errorf("unsupported scorer %q", name)
	}
}

func normalizedScorerName(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" {
		return "bm25"
	}
	return name
}

func copyFieldWeights(in map[string]float64) map[string]float64 {
	return maps.Clone(in)
}
