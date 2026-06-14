package bench

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type Query struct {
	Query          string   `json:"query"`
	RelevantIDs    []string `json:"relevant_ids,omitempty"`
	RelevantTitles []string `json:"relevant_titles,omitempty"`
}

type GroundTruth struct {
	Queries []Query `json:"queries"`
}

func LoadGroundTruth(path string) (*GroundTruth, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open ground truth: %w", err)
	}
	defer f.Close()
	return ReadGroundTruth(f)
}

func ReadGroundTruth(r io.Reader) (*GroundTruth, error) {
	var gt GroundTruth
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&gt); err != nil {
		return nil, fmt.Errorf("decode ground truth: %w", err)
	}
	return &gt, nil
}
