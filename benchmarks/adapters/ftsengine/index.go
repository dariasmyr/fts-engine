package ftsengine

import (
	"fmt"
	"io"
	"sync"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/index/hamtfirst"
)

const hamtFirstIndex = "hamt-first"

var (
	registerBenchmarkCodecsOnce sync.Once
	registerBenchmarkCodecsErr  error
)

func buildIndex(name string) (fts.Index, error) {
	if name == hamtFirstIndex {
		return hamtfirst.New(), nil
	}
	return ftsbuiltin.BuildIndex(name)
}

func registerBenchmarkSnapshotCodecs() error {
	registerBenchmarkCodecsOnce.Do(func() {
		if err := ftsbuiltin.RegisterSnapshotCodecs(); err != nil {
			registerBenchmarkCodecsErr = err
			return
		}
		registerBenchmarkCodecsErr = fts.RegisterIndexSnapshotCodec(
			hamtFirstIndex,
			saveSerializableIndex,
			hamtfirst.Load,
		)
	})
	return registerBenchmarkCodecsErr
}

func saveSerializableIndex(index fts.Index, w io.Writer) error {
	serializable, ok := index.(fts.Serializable)
	if !ok {
		return fmt.Errorf("index does not support serialization")
	}
	return serializable.Serialize(w)
}
