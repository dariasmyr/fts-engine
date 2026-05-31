package fts

import (
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Highlighter struct {
	PreTag       string
	PostTag      string
	MaxFragments int
	FragmentSize int
	Separator    string
	Pipeline     Pipeline
	KeyGen       KeyGenerator
}

type Fragment struct {
	Text    string
	Matches int
}

type highlightPlan struct {
	exactKeys map[string]struct{}
	prefixes  []string
}

type highlightMatch struct {
	start int
	end   int
}

func (h Highlighter) Highlight(query, text string, pipeline Pipeline, keyGen KeyGenerator) []Fragment {
	if text == "" || query == "" {
		return nil
	}

	pre, post, maxFrag, fragSize, sep := h.PreTag, h.PostTag, h.MaxFragments, h.FragmentSize, h.Separator
	if pre == "" {
		pre = "<mark>"
	}
	if post == "" {
		post = "</mark>"
	}
	if maxFrag <= 0 {
		maxFrag = 3
	}
	if fragSize <= 0 {
		fragSize = 150
	}
	if sep == "" {
		sep = " ... "
	}
	if h.Pipeline != nil {
		pipeline = h.Pipeline
	}
	if h.KeyGen != nil {
		keyGen = h.KeyGen
	}
	if pipeline == nil {
		pipeline = defaultPipeline{}
	}
	if keyGen == nil {
		keyGen = WordKeys
	}

	plan := buildHighlightPlan(query, pipeline, keyGen)
	if len(plan.exactKeys) == 0 && len(plan.prefixes) == 0 {
		return nil
	}

	matches := findHighlightMatches(text, pipeline, keyGen, plan)
	if len(matches) == 0 {
		return nil
	}

	clusters := clusterMatches(matches, fragSize)
	sort.SliceStable(clusters, func(i, j int) bool { return len(clusters[i]) > len(clusters[j]) })
	if len(clusters) > maxFrag {
		clusters = clusters[:maxFrag]
	}
	sort.SliceStable(clusters, func(i, j int) bool { return clusters[i][0].start < clusters[j][0].start })

	out := make([]Fragment, 0, len(clusters))
	for _, cluster := range clusters {
		out = append(out, renderFragment(text, cluster, fragSize, pre, post, sep))
	}
	return out
}

func (s *Service) Highlight(query, text string, h Highlighter) []Fragment {
	return h.Highlight(query, text, s.pipeline, s.keyGen)
}

func buildHighlightPlan(query string, pipeline Pipeline, keyGen KeyGenerator) highlightPlan {
	if parsed, err := ParseQuery(query); err == nil {
		plan := highlightPlan{exactKeys: make(map[string]struct{})}
		collectHighlightTerms(parsed, pipeline, keyGen, &plan, false)
		if len(plan.exactKeys) > 0 || len(plan.prefixes) > 0 {
			return plan
		}
	}

	return highlightPlan{exactKeys: buildKeySet(query, pipeline, keyGen)}
}

func collectHighlightTerms(q Query, pipeline Pipeline, keyGen KeyGenerator, plan *highlightPlan, exclude bool) {
	switch query := q.(type) {
	case nil:
		return
	case TermQuery:
		if !exclude {
			addExactTextKeys(query.Term, pipeline, keyGen, plan.exactKeys)
		}
	case *TermQuery:
		if query != nil && !exclude {
			addExactTextKeys(query.Term, pipeline, keyGen, plan.exactKeys)
		}
	case PhraseQuery:
		if !exclude {
			addExactTextKeys(query.Phrase, pipeline, keyGen, plan.exactKeys)
		}
	case *PhraseQuery:
		if query != nil && !exclude {
			addExactTextKeys(query.Phrase, pipeline, keyGen, plan.exactKeys)
		}
	case PrefixQuery:
		if !exclude {
			addPrefixes(query.Prefix, pipeline, plan)
		}
	case *PrefixQuery:
		if query != nil && !exclude {
			addPrefixes(query.Prefix, pipeline, plan)
		}
	case *BooleanQuery:
		if query == nil {
			return
		}
		for _, clause := range query.Clauses {
			collectHighlightTerms(clause.Query, pipeline, keyGen, plan, exclude || clause.Occur == MustNot)
		}
	}
}

func addExactTextKeys(text string, pipeline Pipeline, keyGen KeyGenerator, out map[string]struct{}) {
	for _, token := range pipeline.Process(text) {
		keys, err := keyGen(token)
		if err != nil {
			continue
		}
		for _, key := range keys {
			out[key] = struct{}{}
		}
	}
}

func addPrefixes(text string, pipeline Pipeline, plan *highlightPlan) {
	for _, token := range pipeline.Process(text) {
		if token == "" {
			continue
		}
		plan.prefixes = append(plan.prefixes, token)
	}
}

func buildKeySet(query string, pipeline Pipeline, keyGen KeyGenerator) map[string]struct{} {
	tokens := pipeline.Process(query)
	if len(tokens) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		keys, err := keyGen(token)
		if err != nil {
			continue
		}
		for _, key := range keys {
			out[key] = struct{}{}
		}
	}
	return out
}

func findHighlightMatches(text string, pipeline Pipeline, keyGen KeyGenerator, plan highlightPlan) []highlightMatch {
	var matches []highlightMatch
	wordStart := -1
	emit := func(end int) {
		if wordStart < 0 {
			return
		}
		word := text[wordStart:end]
		if isHighlightHit(word, pipeline, keyGen, plan) {
			matches = append(matches, highlightMatch{start: wordStart, end: end})
		}
		wordStart = -1
	}

	for i, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if wordStart < 0 {
				wordStart = i
			}
			continue
		}
		emit(i)
	}
	emit(len(text))
	return matches
}

func isHighlightHit(word string, pipeline Pipeline, keyGen KeyGenerator, plan highlightPlan) bool {
	tokens := pipeline.Process(word)
	for _, token := range tokens {
		for _, prefix := range plan.prefixes {
			if strings.HasPrefix(token, prefix) {
				return true
			}
		}
		keys, err := keyGen(token)
		if err != nil {
			continue
		}
		for _, key := range keys {
			if _, ok := plan.exactKeys[key]; ok {
				return true
			}
		}
	}
	return false
}

func clusterMatches(matches []highlightMatch, fragSize int) [][]highlightMatch {
	var clusters [][]highlightMatch
	var current []highlightMatch
	for _, match := range matches {
		if len(current) == 0 {
			current = []highlightMatch{match}
			continue
		}
		if match.end-current[0].start <= fragSize {
			current = append(current, match)
			continue
		}
		clusters = append(clusters, current)
		current = []highlightMatch{match}
	}
	if len(current) > 0 {
		clusters = append(clusters, current)
	}
	return clusters
}

func renderFragment(text string, cluster []highlightMatch, fragSize int, pre, post, sep string) Fragment {
	first, last := cluster[0].start, cluster[len(cluster)-1].end
	span := last - first
	pad := (fragSize - span) / 2
	if pad < 0 {
		pad = 0
	}
	left := first - pad
	right := last + pad
	if left < 0 {
		left = 0
	}
	if right > len(text) {
		right = len(text)
	}
	left = expandToWordBoundary(text, left, -1)
	right = expandToWordBoundary(text, right, 1)

	var b strings.Builder
	b.Grow(right - left + len(cluster)*(len(pre)+len(post)) + len(sep)*2)
	if left > 0 {
		b.WriteString(sep)
	}
	cursor := left
	for _, match := range cluster {
		if match.start < cursor {
			continue
		}
		b.WriteString(text[cursor:match.start])
		b.WriteString(pre)
		b.WriteString(text[match.start:match.end])
		b.WriteString(post)
		cursor = match.end
	}
	b.WriteString(text[cursor:right])
	if right < len(text) {
		b.WriteString(sep)
	}
	return Fragment{Text: b.String(), Matches: len(cluster)}
}

func expandToWordBoundary(text string, pos, dir int) int {
	if dir < 0 {
		for pos > 0 {
			r, size := utf8.DecodeLastRuneInString(text[:pos])
			if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
				break
			}
			pos -= size
		}
		return pos
	}
	for pos < len(text) {
		r, size := utf8.DecodeRuneInString(text[pos:])
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			break
		}
		pos += size
	}
	return pos
}
