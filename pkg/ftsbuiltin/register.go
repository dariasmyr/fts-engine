package ftsbuiltin

import (
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/filter"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/hamt"
	"github.com/dariasmyr/fts-engine/pkg/index/hamtpointered"
	"github.com/dariasmyr/fts-engine/pkg/index/radix"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/index/trigram"
)

type FilterOptions struct {
	BloomExpectedItems uint64
	BloomBitsPerItem   uint64
	BloomK             uint64

	CuckooBucketCount int
	CuckooBucketSize  int
	CuckooMaxKicks    int

	RibbonExpectedItems uint32
	RibbonExtraCells    uint32
	RibbonWindowSize    uint32
	RibbonSeed          uint64
	RibbonMaxAttempts   uint32
}

func BuildIndex(name string) (fts.Index, error) {
	switch name {
	case "radix":
		return radix.New(), nil
	case "slicedradix":
		return slicedradix.New(), nil
	case "hamt":
		return hamt.New(), nil
	case "hamtpointered":
		return hamtpointered.New(), nil
	case "trigram":
		return trigram.New(), nil
	default:
		return nil, fmt.Errorf("unknown index %q", name)
	}
}

func BuildFilter(name string, opts FilterOptions) (fts.Filter, error) {
	switch name {
	case "", "none":
		return nil, nil
	case "bloom":
		return filter.NewBloomFilter(opts.BloomExpectedItems, opts.BloomBitsPerItem, opts.BloomK), nil
	case "cuckoo":
		return filter.NewCuckooFilter(opts.CuckooBucketCount, opts.CuckooBucketSize, opts.CuckooMaxKicks), nil
	case "ribbon":
		rf, err := filter.NewRibbonFilter(opts.RibbonExpectedItems, opts.RibbonExtraCells, opts.RibbonWindowSize, opts.RibbonSeed)
		if err != nil {
			return nil, err
		}

		return fts.NewBufferedStaticFilterWithRetries(rf, opts.RibbonMaxAttempts), nil
	default:
		return nil, fmt.Errorf("unknown filter %q", name)
	}
}
