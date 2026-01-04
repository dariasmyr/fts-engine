package radixtrie

import (
	"context"
	"fmt"
	"fts-hw/internal/domain/models"
	utils "fts-hw/internal/utils/format"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	snowballeng "github.com/kljensen/snowball/english"
)

type Node struct {
	prefix   string
	terminal bool
	docs     map[string]int
	children []*Node
}

func newNode(prefix string) *Node {
	return &Node{
		prefix: prefix,
		docs:   make(map[string]int),
	}
}

type Trie struct {
	root *Node
	mu   sync.RWMutex
}

func NewTrie() *Trie {
	return &Trie{
		root: newNode(""),
	}
}

// longest common prefix
func lcp(a, b string) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}

func (t *Trie) Insert(word string, docID string) error {

	t.mu.Lock()
	defer t.mu.Unlock()

	current := t.root
	rest := word

	var node *Node

	for {
		for i, child := range current.children {
			p := lcp(rest, child.prefix)

			if p == 0 {
				continue
			}

			// prefix fully matched with child - go deeper
			if p == len(child.prefix) {
				current = child
				rest = rest[p:]

				if rest == "" {
					current.terminal = true
					current.docs[docID]++
					return nil
				}

				goto NEXT
			}

			// split
			common := child.prefix[:p]
			childSuffix := child.prefix[p:]
			newSuffix := rest[p:]

			middle := newNode(common)

			// shorten old child prefix
			child.prefix = childSuffix

			// relink old node
			middle.children = append(middle.children, child)

			// replace child with middle node (with common suffix)
			current.children[i] = middle

			// if rest is not empty, create new node and mark it as end for new word
			if newSuffix != "" {
				node = newNode(newSuffix)
				node.terminal = true
				node.docs[docID]++
				middle.children = append(middle.children, node)
				return nil
			}

			// rest is empty, mark middle common node as end for new word
			middle.terminal = true
			middle.docs[docID]++
			return nil
		}

		//if no child fitted new word by prefix - jist add new node
		node = newNode(rest)
		node.terminal = true
		node.docs[docID]++
		current.children = append(current.children, node)
		return nil

	NEXT:
	}
}

func (t *Trie) Search(word string) (map[string]int, error) {

	t.mu.RLock()
	defer t.mu.RUnlock()

	current := t.root
	rest := word

	for {
		// breakpoint to mark that we found a match of current level
		found := false

		for _, child := range current.children {
			p := lcp(rest, child.prefix)

			// case 0:
			// no common prefix at all - try next child
			if p == 0 {
				continue
			}

			// case 1:
			// the remaining search word is fully consumed
			// (word is shorter than or equal to the node prefix)
			// - the word is considered found only if this node is terminal
			if p == len(rest) {
				if child.terminal {
					result := make(map[string]int, len(child.docs))
					for k, v := range child.docs {
						result[k] = v
					}
					return result, nil
				}
				// query word matched only a prefix of a longer word in a tree
				return nil, nil
			}

			// case 2:
			// the node prefix is fully matched,
			// but the word still has remaining characters
			// - continue searching deeper in the trie
			if p == len(child.prefix) {
				current = child
				rest = rest[p:]
				found = true
				break
			}

			// case 3:
			// partial overlap:
			// 0 < p < len(rest) AND 0 < p < len(child.prefix)
			// - the word does not exist in the trie
			return nil, nil
		}

		// no child had a common prefix with the remaining word
		if !found {
			return nil, nil
		}
	}
}

func tokenize(content string) []string {
	lastSplit := 0
	tokens := make([]string, 0)
	for i, char := range content {
		if char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z' {
			continue
		}

		if i-lastSplit != 0 {
			tokens = append(tokens, content[lastSplit:i])
		}

		charBytes := utf8.RuneLen(char)
		// Update lastSplit considering the byte length of the character
		// We don't use `i + 1` because characters can occupy more than one byte in UTF-8.
		lastSplit = i + charBytes // account for the character's byte length
	}

	if len(content) > lastSplit {
		tokens = append(tokens, content[lastSplit:])
	}

	return tokens
}

func (t *Trie) IndexDocument(docID string, content string) {
	tokens := tokenize(content)
	for _, token := range tokens {

		// skip stop words
		if snowballeng.IsStopWord(token) {
			continue
		}

		//lowercase and stemmimg (eng only)
		token = snowballeng.Stem(token, false)

		err := t.Insert(token, docID)
		if err != nil {
			fmt.Println(err)
			continue
		}

	}
}

func (t *Trie) SearchDocuments(ctx context.Context, query string, maxResults int) (*models.SearchResult, error) {
	startTime := time.Now()
	timings := make(map[string]string)

	preprocessStart := time.Now()
	tokens := tokenize(query)
	timings["preprocess"] = utils.FormatDuration(time.Since(preprocessStart))

	searchStart := time.Now()

	docUniqueMatches := make(map[string]int)
	docTotalMatches := make(map[string]int)

	for _, token := range tokens {
		// skip stop words
		if snowballeng.IsStopWord(token) {
			continue
		}
		//lowercase and stemmimg (eng only)
		token = snowballeng.Stem(token, false)

		docEntries, err := t.Search(token)
		if err != nil {
			return nil, err
		}
		if docEntries == nil {
			continue
		}
		for docID, count := range docEntries {
			docUniqueMatches[docID]++
			docTotalMatches[docID] += count
		}

	}

	if len(docUniqueMatches) < maxResults {
		maxResults = len(docUniqueMatches)
	}

	results := make([]models.ResultData, 0, len(docUniqueMatches))
	for docID, uniqueMatches := range docUniqueMatches {
		results = append(results, models.ResultData{
			ID:            docID,
			UniqueMatches: uniqueMatches,
			TotalMatches:  docTotalMatches[docID],
			Document:      models.Document{},
		})
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].UniqueMatches == results[j].UniqueMatches {
			return results[i].TotalMatches > results[j].TotalMatches
		}
		return results[i].UniqueMatches > results[j].UniqueMatches
	})

	timings["search_tokens"] = utils.FormatDuration(time.Since(searchStart))

	timings["total"] = utils.FormatDuration(time.Since(startTime))

	var lastIndex int
	lastIndex = maxResults

	if len(docUniqueMatches) > maxResults {
		lastIndex = len(docUniqueMatches)
	}

	return &models.SearchResult{
		ResultData:        results[:lastIndex],
		Timings:           timings,
		TotalResultsCount: len(docUniqueMatches),
	}, nil
}
