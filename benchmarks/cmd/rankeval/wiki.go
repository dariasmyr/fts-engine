package main

import (
	"compress/bzip2"
	"compress/gzip"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"unicode"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftseval"
)

type wikiEvalPage struct {
	Title    string           `xml:"title"`
	ID       string           `xml:"id"`
	Redirect *struct{}        `xml:"redirect"`
	Revision wikiEvalRevision `xml:"revision"`
}

type wikiEvalRevision struct {
	Text string `xml:"text"`
}

type wikiTermDocs struct {
	title map[fts.DocID]int
	body  map[fts.DocID]int
}

func wikiMultifield(path string, maxDocs int, queryCount int) ([]fts.Document, []ftseval.Query, error) {
	r, err := openMaybeCompressed(path)
	if err != nil {
		return nil, nil, err
	}
	defer r.Close()

	dec := xml.NewDecoder(r)
	docs := make([]fts.Document, 0, maxDocs)
	terms := make(map[string]*wikiTermDocs)
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("rankeval: parse wiki XML: %w", err)
		}
		start, ok := tok.(xml.StartElement)
		if !ok || start.Name.Local != "page" {
			continue
		}
		var page wikiEvalPage
		if err := dec.DecodeElement(&page, &start); err != nil {
			return nil, nil, fmt.Errorf("rankeval: decode wiki page: %w", err)
		}
		if page.Redirect != nil {
			continue
		}
		title := strings.TrimSpace(page.Title)
		body := strings.TrimSpace(page.Revision.Text)
		if title == "" || body == "" {
			continue
		}
		docID := fts.DocID(strings.TrimSpace(page.ID))
		if docID == "" {
			docID = fts.DocID(title)
		}
		docs = append(docs, fts.Document{ID: docID, Fields: map[string]fts.Field{
			"title": {Value: title},
			"body":  {Value: body},
		}})
		observeWikiTerms(terms, tokenizeEvalText(title), docID, true)
		observeWikiTerms(terms, tokenizeEvalText(body), docID, false)
		if maxDocs > 0 && len(docs) >= maxDocs {
			break
		}
	}
	if len(docs) == 0 {
		return nil, nil, fmt.Errorf("rankeval: no wiki documents loaded from %s", path)
	}
	queries := buildWikiMultifieldQueries(terms, queryCount)
	if len(queries) == 0 {
		return nil, nil, fmt.Errorf("rankeval: no wiki queries generated from %s", path)
	}
	return docs, queries, nil
}

func observeWikiTerms(dst map[string]*wikiTermDocs, tokens []string, docID fts.DocID, title bool) {
	seen := make(map[string]int, len(tokens))
	for _, token := range tokens {
		seen[token]++
	}
	for token, count := range seen {
		entry := dst[token]
		if entry == nil {
			entry = &wikiTermDocs{title: make(map[fts.DocID]int), body: make(map[fts.DocID]int)}
			dst[token] = entry
		}
		if title {
			entry.title[docID] += count
		} else {
			entry.body[docID] += count
		}
	}
}

func buildWikiMultifieldQueries(terms map[string]*wikiTermDocs, limit int) []ftseval.Query {
	type candidate struct {
		term      string
		titleDocs int
		bodyOnly  int
		maxBodyTF int
		bodyTF    int
	}
	candidates := make([]candidate, 0, len(terms))
	for term, docs := range terms {
		if len(term) < 4 || len(docs.title) == 0 {
			continue
		}
		var bodyOnly, maxBodyTF, bodyTF int
		for id, tf := range docs.body {
			if _, inTitle := docs.title[id]; !inTitle {
				bodyOnly++
				bodyTF += tf
				if tf > maxBodyTF {
					maxBodyTF = tf
				}
			}
		}
		if bodyOnly == 0 {
			continue
		}
		candidates = append(candidates, candidate{
			term:      term,
			titleDocs: len(docs.title),
			bodyOnly:  bodyOnly,
			maxBodyTF: maxBodyTF,
			bodyTF:    bodyTF,
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].maxBodyTF != candidates[j].maxBodyTF {
			return candidates[i].maxBodyTF > candidates[j].maxBodyTF
		}
		if candidates[i].bodyTF != candidates[j].bodyTF {
			return candidates[i].bodyTF > candidates[j].bodyTF
		}
		if candidates[i].bodyOnly != candidates[j].bodyOnly {
			return candidates[i].bodyOnly > candidates[j].bodyOnly
		}
		if candidates[i].titleDocs != candidates[j].titleDocs {
			return candidates[i].titleDocs < candidates[j].titleDocs
		}
		return candidates[i].term < candidates[j].term
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	queries := make([]ftseval.Query, 0, len(candidates))
	for _, item := range candidates {
		docs := terms[item.term]
		relevant := make(map[fts.DocID]float64, len(docs.title)+len(docs.body))
		for id := range docs.body {
			relevant[id] = 1
		}
		for id := range docs.title {
			relevant[id] = 3
		}
		queries = append(queries, ftseval.Query{Name: item.term, Query: item.term, Relevant: relevant})
	}
	return queries
}

func openMaybeCompressed(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("rankeval: open %s: %w", path, err)
	}
	switch {
	case strings.HasSuffix(path, ".gz"):
		gz, err := gzip.NewReader(f)
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("rankeval: open gzip %s: %w", path, err)
		}
		return combinedReadCloser{Reader: gz, closers: []io.Closer{gz, f}}, nil
	case strings.HasSuffix(path, ".bz2"):
		return combinedReadCloser{Reader: bzip2.NewReader(f), closers: []io.Closer{f}}, nil
	default:
		return f, nil
	}
}

type combinedReadCloser struct {
	io.Reader
	closers []io.Closer
}

func (c combinedReadCloser) Close() error {
	var first error
	for _, closer := range c.closers {
		if err := closer.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func tokenizeEvalText(text string) []string {
	return strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}
