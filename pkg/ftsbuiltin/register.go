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
	"github.com/dariasmyr/fts-engine/pkg/index/trigram"
)

type FilterOptions struct {
	BloomExpectedItems uint64
	BloomBitsPerItem   uint64
	BloomK             uint64

	CuckooBucketCount int
	CuckooBucketSize  int
	CuckooMaxKicks    int
}

var (
	registerIndexesOnce   sync.Once
	registerIndexesErr    error
	registerFiltersOnce   sync.Once
	registerFiltersErr    error
	registerSnapshotsOnce sync.Once
	registerSnapshotsErr  error
)

func RegisterIndexes() error {
	registerIndexesOnce.Do(func() {
		registerIndexesErr = registerIndexes()
	})
	return registerIndexesErr
}

func RegisterFilters(opts FilterOptions) error {
	registerFiltersOnce.Do(func() {
		registerFiltersErr = registerFilters(opts)
	})
	return registerFiltersErr
}

func RegisterSnapshotCodecs() error {
	registerSnapshotsOnce.Do(func() {
		registerSnapshotsErr = registerSnapshotCodecs()
	})
	return registerSnapshotsErr
}

func registerIndexes() error {
	if err := fts.RegisterIndex("radix", func() (fts.Index, error) { return radix.New(), nil }); err != nil {
		return err
	}
	if err := fts.RegisterIndex("slicedradix", func() (fts.Index, error) { return slicedradix.New(), nil }); err != nil {
		return err
	}
	if err := fts.RegisterIndex("hamt", func() (fts.Index, error) { return hamt.New(), nil }); err != nil {
		return err
	}
	if err := fts.RegisterIndex("hamtpointered", func() (fts.Index, error) { return hamtpointered.New(), nil }); err != nil {
		return err
	}
	if err := fts.RegisterIndex("trigram", func() (fts.Index, error) { return trigram.New(), nil }); err != nil {
		return err
	}

	return nil
}

func registerFilters(opts FilterOptions) error {
	if err := fts.RegisterFilter("bloom", func() (fts.Filter, error) {
		return filter.NewBloomFilter(opts.BloomExpectedItems, opts.BloomBitsPerItem, opts.BloomK), nil
	}); err != nil {
		return err
	}

	if err := fts.RegisterFilter("cuckoo", func() (fts.Filter, error) {
		return filter.NewCuckooFilter(opts.CuckooBucketCount, opts.CuckooBucketSize, opts.CuckooMaxKicks), nil
	}); err != nil {
		return err
	}

	return nil
}

func registerSnapshotCodecs() error {
	if err := registerIndexCodec("radix", radix.Load); err != nil {
		return err
	}
	if err := registerIndexCodec("slicedradix", slicedradix.Load); err != nil {
		return err
	}
	if err := registerIndexCodec("hamt", hamt.Load); err != nil {
		return err
	}
	if err := registerIndexCodec("hamtpointered", hamtpointered.Load); err != nil {
		return err
	}
	if err := registerIndexCodec("trigram", trigram.Load); err != nil {
		return err
	}

	if err := registerFilterCodec("bloom", func(r io.Reader) (fts.Filter, error) {
		return filter.LoadBloomFilter(r)
	}); err != nil {
		return err
	}

	if err := registerFilterCodec("cuckoo", func(r io.Reader) (fts.Filter, error) {
		return filter.LoadCuckooFilter(r)
	}); err != nil {
		return err
	}

	return nil
}

func registerIndexCodec(name string, loader fts.IndexSnapshotLoader) error {
	return fts.RegisterIndexSnapshotCodec(name,
		func(index fts.Index, w io.Writer) error {
			serializable, ok := index.(fts.Serializable)
			if !ok {
				return fmt.Errorf("index %q does not support serialization", name)
			}
			return serializable.Serialize(w)
		},
		loader,
	)
}

func registerFilterCodec(name string, loader fts.FilterSnapshotLoader) error {
	return fts.RegisterFilterSnapshotCodec(name,
		func(value fts.Filter, w io.Writer) error {
			serializable, ok := value.(fts.Serializable)
			if !ok {
				return fmt.Errorf("filter %q does not support serialization", name)
			}
			return serializable.Serialize(w)
		},
		loader,
	)
}
