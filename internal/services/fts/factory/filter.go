package factory

import (
	"fmt"
	"fts-hw/internal/services/fts"
	"fts-hw/internal/services/fts/filters"
)

type FilterOptions struct {
	Type   string
	Bloom  filters.BloomConfig
	Cuckoo filters.CuckooConfig
	Ribbon filters.RibbonConfig
}

func NewFilter(opts FilterOptions) (fts.TermFilter, error) {
	switch opts.Type {
	case "none":
		return filters.NewNoopFilter(), nil
	case "bloom":
		return filters.NewBloomFilter(opts.Bloom), nil
	case "cuckoo":
		return filters.NewCuckooFilter(opts.Cuckoo), nil
	case "ribbon":
		return filters.NewRibbonFilter(opts.Ribbon), nil
	default:
		return nil, fmt.Errorf("unknown filter type: %s", opts.Type)
	}
}
