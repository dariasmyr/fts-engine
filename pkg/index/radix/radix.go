package radix

import (
	internalfts "fts-hw/internal/services/fts"
	radixtrie "fts-hw/internal/services/fts/radix"
	"fts-hw/internal/utils"
	"fts-hw/pkg/fts"
)

type Index struct {
	trie *radixtrie.Trie
}

func New() *Index {
	return &Index{trie: radixtrie.New()}
}

func (i *Index) Insert(key string, id fts.DocID) error {
	return i.trie.Insert(key, string(id))
}

func (i *Index) Search(key string) ([]fts.DocRef, error) {
	docs, err := i.trie.Search(key)
	if err != nil {
		return nil, err
	}

	return toDocRefs(docs), nil
}

func (i *Index) Analyze() fts.Stats {
	return toStats(i.trie.Analyze())
}

func toDocRefs(docs []internalfts.Document) []fts.DocRef {
	res := make([]fts.DocRef, 0, len(docs))
	for _, doc := range docs {
		res = append(res, fts.DocRef{ID: fts.DocID(doc.ID), Count: uint32(doc.Count)})
	}
	return res
}

func toStats(s utils.TrieStats) fts.Stats {
	return fts.Stats{
		Nodes:               s.Nodes,
		Leaves:              s.Leaves,
		MaxDepth:            s.MaxDepth,
		AvgDepth:            s.AvgDepth,
		TotalDocs:           s.TotalDocs,
		AvgChildrenPerLevel: append([]float64(nil), s.AvgChildrenPerLevel...),
		TotalChildren:       s.TotalChildren,
	}
}
