package trigram

import (
	trigramtrie "fts-hw/internal/services/fts/trigram"
	"fts-hw/pkg/fts"
)

type Index struct {
	trie *trigramtrie.Trie
}

func New() *Index {
	return &Index{trie: trigramtrie.New()}
}

func (i *Index) Insert(key string, id fts.DocID) error {
	return i.trie.Insert(key, string(id))
}

func (i *Index) Search(key string) ([]fts.DocRef, error) {
	docs, err := i.trie.Search(key)
	if err != nil {
		return nil, err
	}

	res := make([]fts.DocRef, 0, len(docs))
	for _, doc := range docs {
		res = append(res, fts.DocRef{ID: fts.DocID(doc.ID), Count: uint32(doc.Count)})
	}

	return res, nil
}

func (i *Index) Analyze() fts.Stats {
	s := i.trie.Analyze()
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

var _ fts.Index = (*Index)(nil)
