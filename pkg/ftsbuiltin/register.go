package ftsbuiltin

import (
	"fmt"
	"io"
	"sync"

	"github.com/dariasmyr/fts-engine/pkg/filter"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/hamt"
	"github.com/dariasmyr/fts-engine/pkg/index/hamtpointered"
	"github.com/dariasmyr/fts-engine/pkg/index/radix"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
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

var (
	registerSnapshotCodecsOnce sync.Once
	registerSnapshotCodecsErr  error
)

func RegisterSnapshotCodecs() error {
	registerSnapshotCodecsOnce.Do(func() {
		registerSnapshotCodecsErr = registerSnapshotCodecs()
	})

	return registerSnapshotCodecsErr
}

func registerSnapshotCodecs() error {
	if err := RegisterIndexes(); err != nil {
		return err
	}

	if err := RegisterFilters(); err != nil {
		return err
	}

	return nil
}

func RegisterIndexes() error {
	if err := fts.RegisterIndexSnapshotCodec("radix", saveSerializableIndex, radix.Load); err != nil {
		return err
	}
	if err := fts.RegisterIndexSnapshotCodec("slicedradix", saveSerializableIndex, slicedradix.Load); err != nil {
		return err
	}
	if err := fts.RegisterIndexSnapshotCodec("hamt", saveSerializableIndex, hamt.Load); err != nil {
		return err
	}
	if err := fts.RegisterIndexSnapshotCodec("hamtpointered", saveSerializableIndex, hamtpointered.Load); err != nil {
		return err
	}

	return nil
}

func RegisterFilters() error {
	if err := fts.RegisterFilterSnapshotCodec("bloom", saveSerializableFilter, loadBloomFilter); err != nil {
		return err
	}
	if err := fts.RegisterFilterSnapshotCodec("cuckoo", saveSerializableFilter, loadCuckooFilter); err != nil {
		return err
	}
	if err := fts.RegisterFilterSnapshotCodec("ribbon", saveSerializableFilter, loadRibbonFilter); err != nil {
		return err
	}

	return nil
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

func saveSerializableIndex(index fts.Index, w io.Writer) error {
	serializable, ok := index.(fts.Serializable)
	if !ok {
		return fmt.Errorf("index does not support serialization")
	}

	return serializable.Serialize(w)
}

func saveSerializableFilter(searchFilter fts.Filter, w io.Writer) error {
	serializable, ok := searchFilter.(fts.Serializable)
	if !ok {
		return fmt.Errorf("filter does not support serialization")
	}

	return serializable.Serialize(w)
}

func loadRibbonFilter(r io.Reader) (fts.Filter, error) {
	rf, err := filter.LoadRibbonFilter(r)
	if err != nil {
		return nil, err
	}

	wrapped := fts.NewBufferedStaticFilter(rf)
	if err = wrapped.Build(); err != nil {
		return nil, err
	}

	return wrapped, nil
}

func loadBloomFilter(r io.Reader) (fts.Filter, error) {
	return filter.LoadBloomFilter(r)
}

func loadCuckooFilter(r io.Reader) (fts.Filter, error) {
	return filter.LoadCuckooFilter(r)
}
