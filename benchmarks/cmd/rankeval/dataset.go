package main

import (
	"fmt"
	"strings"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftseval"
)

func loadDataset(cfg config) ([]fts.Document, []ftseval.Query, map[string]any, error) {
	switch cfg.Dataset {
	case "multifield-synthetic":
		docs, queries := multifieldSynthetic(cfg.Queries)
		return docs, queries, map[string]any{
			"name":    cfg.Dataset,
			"queries": cfg.Queries,
			"fields":  []string{"title", "tags", "body"},
		}, nil
	case "wiki-multifield":
		docs, queries, err := wikiMultifield(cfg.WikiDump, cfg.MaxDocs, cfg.Queries)
		if err != nil {
			return nil, nil, nil, err
		}
		return docs, queries, map[string]any{
			"name":      cfg.Dataset,
			"wiki_dump": cfg.WikiDump,
			"max_docs":  cfg.MaxDocs,
			"docs":      len(docs),
			"queries":   len(queries),
			"fields":    []string{"title", "body"},
		}, nil
	default:
		return nil, nil, nil, fmt.Errorf("rankeval: unsupported -dataset %q", cfg.Dataset)
	}
}

func multifieldSynthetic(queryCount int) ([]fts.Document, []ftseval.Query) {
	docs := make([]fts.Document, 0, queryCount*4)
	queries := make([]ftseval.Query, 0, queryCount)
	for i := range queryCount {
		topic := fmt.Sprintf("topic%04d", i)
		titleID := fts.DocID(fmt.Sprintf("doc-%04d-title", i))
		tagsID := fts.DocID(fmt.Sprintf("doc-%04d-tags", i))
		bodyID := fts.DocID(fmt.Sprintf("doc-%04d-body", i))
		noiseID := fts.DocID(fmt.Sprintf("doc-%04d-body-noise", i))

		docs = append(docs,
			fts.Document{ID: titleID, Fields: map[string]fts.Field{
				"title": {Value: topic + " guide"},
				"tags":  {Value: "reference"},
				"body":  {Value: "short overview"},
			}},
			fts.Document{ID: tagsID, Fields: map[string]fts.Field{
				"title": {Value: "reference guide"},
				"tags":  {Value: topic},
				"body":  {Value: "short overview"},
			}},
			fts.Document{ID: bodyID, Fields: map[string]fts.Field{
				"title": {Value: "reference guide"},
				"tags":  {Value: "reference"},
				"body":  {Value: strings.Repeat(topic+" ", 3)},
			}},
			fts.Document{ID: noiseID, Fields: map[string]fts.Field{
				"title": {Value: "reference guide"},
				"tags":  {Value: "reference"},
				"body":  {Value: strings.Repeat(topic+" ", 8)},
			}},
		)
		queries = append(queries, ftseval.Query{
			Name:  topic,
			Query: topic,
			Relevant: map[fts.DocID]float64{
				titleID: 3,
				tagsID:  2,
				bodyID:  1,
			},
		})
	}
	return docs, queries
}
