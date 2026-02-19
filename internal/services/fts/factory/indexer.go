package factory

import (
	"fmt"
	"fts-hw/internal/services/fts"
	hamt "fts-hw/internal/services/fts/indexers/hamt"
	hamtpointered "fts-hw/internal/services/fts/indexers/hamtpointered"
	radixtrie "fts-hw/internal/services/fts/indexers/radix"
	radixtriesliced "fts-hw/internal/services/fts/indexers/slicedradix"
	trigramtrie "fts-hw/internal/services/fts/indexers/trigram"
)

func NewIndexer(indexerType string) (fts.Index, fts.KeyGenerator, error) {
	switch indexerType {
	case "radix":
		return radixtrie.New(), fts.WordKeys, nil
	case "slicedradix":
		return radixtriesliced.New(), fts.WordKeys, nil
	case "hamt":
		return hamt.New(), fts.WordKeys, nil
	case "hamtpointered":
		return hamtpointered.New(), fts.WordKeys, nil
	case "trigram":
		return trigramtrie.New(), trigramtrie.TrigramKeys, nil
	default:
		return nil, nil, fmt.Errorf("unknown indexer type: %s", indexerType)
	}
}
