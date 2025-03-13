package models

import "time"

type DocumentBase struct {
	Title    string `xml:"title" json:"title"`
	URL      string `xml:"url" json:"url"`
	Abstract string `xml:"abstract" json:"abstract"`
}

type Document struct {
	DocumentBase
	ID      string `json:"id"`
	Extract string `json:"extract"`
}

type ResultData struct {
	ID            string   `json:"id"`
	UniqueMatches int      `json:"unique_matches"`
	TotalMatches  int      `json:"total_matches"`
	Document      Document `json:"document"`
}

type SearchResult struct {
	ResultData        []ResultData
	TotalResultsCount int
	Timings           map[string]time.Duration
}
