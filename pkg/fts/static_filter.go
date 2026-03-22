package fts

import (
	"fmt"
	"io"
	"sync"
)

// BufferedStaticFilter apapts StaticFilter to Filter interface.
// Key is added on every Add(), but in the end the final build is called by Build().
type BufferedStaticFilter struct {
	mu     sync.Mutex
	static StaticFilter
	keys   map[string]struct{}
	dirty  bool
	built  bool
	tries  uint32
}

func (f *BufferedStaticFilter) BuildFromKeys(iterate func(func(string) bool) error) error {
	if iterate == nil {
		return fmt.Errorf("fts: buffered static filter: nil key iterator")
	}

	return f.BuildFromKeyStream(keyIteratorToStream(iterate))
}

func (f *BufferedStaticFilter) BuildFromKeyStream(stream func(func([]byte) bool) error) error {
	if f == nil || f.static == nil {
		return nil
	}
	if stream == nil {
		return fmt.Errorf("fts: buffered static filter: nil key stream")
	}

	if retryable, ok := f.static.(RetryableStaticFilter); ok && f.tries > 1 {
		if err := retryable.BuildWithRetriesFromKeyStream(stream, f.tries); err != nil {
			return err
		}
		f.markBuiltAndResetBuffer()
		return nil
	}

	if err := f.static.BuildFromKeyStream(stream); err != nil {
		return err
	}
	f.markBuiltAndResetBuffer()
	return nil
}

func keyIteratorToStream(iterate func(func(string) bool) error) func(func([]byte) bool) error {
	return func(emit func([]byte) bool) error {
		return iterate(func(key string) bool {
			return emit([]byte(key))
		})
	}
}

func NewBufferedStaticFilter(static StaticFilter) *BufferedStaticFilter {
	return NewBufferedStaticFilterWithRetries(static, 1)
}

func NewBufferedStaticFilterWithRetries(static StaticFilter, maxAttempts uint32) *BufferedStaticFilter {
	if maxAttempts == 0 {
		maxAttempts = 1
	}

	return &BufferedStaticFilter{
		static: static,
		keys:   make(map[string]struct{}),
		tries:  maxAttempts,
	}
}

func (f *BufferedStaticFilter) Add(item []byte) bool {
	if f == nil || f.static == nil || len(item) == 0 {
		return true
	}

	f.mu.Lock()
	key := string(item)
	if _, exists := f.keys[key]; !exists {
		f.keys[key] = struct{}{}
		f.dirty = true
	}
	f.mu.Unlock()

	return true
}

func (f *BufferedStaticFilter) Contains(item []byte) bool {
	if f == nil || f.static == nil {
		return true
	}

	f.mu.Lock()
	dirty := f.dirty
	built := f.built
	f.mu.Unlock()

	if dirty || !built {
		return true
	}

	return f.static.Contains(item)
}

func (f *BufferedStaticFilter) Build() error {
	if f == nil || f.static == nil {
		return nil
	}

	f.mu.Lock()
	if !f.dirty && f.built {
		f.mu.Unlock()
		return nil
	}

	keys := make([]string, 0, len(f.keys))
	for key := range f.keys {
		keys = append(keys, key)
	}
	f.mu.Unlock()

	if len(keys) == 0 {
		f.mu.Lock()
		f.dirty = false
		f.built = true
		f.mu.Unlock()
		return nil
	}

	err := f.BuildFromKeyStream(stringSliceToKeyStream(keys))
	if err != nil {
		f.mu.Lock()
		f.built = false
		f.mu.Unlock()
		return err
	}

	f.mu.Lock()
	f.dirty = false
	f.built = true
	f.mu.Unlock()

	return nil
}

func stringSliceToKeyStream(keys []string) func(func([]byte) bool) error {
	return func(emit func([]byte) bool) error {
		for _, key := range keys {
			if !emit([]byte(key)) {
				break
			}
		}
		return nil
	}
}

func (f *BufferedStaticFilter) markBuiltAndResetBuffer() {
	f.mu.Lock()
	f.keys = make(map[string]struct{})
	f.dirty = false
	f.built = true
	f.mu.Unlock()
}

func (f *BufferedStaticFilter) Serialize(w io.Writer) error {
	if f == nil || f.static == nil {
		return fmt.Errorf("fts: buffered static filter: nil static filter")
	}

	serializable, ok := f.static.(Serializable)
	if !ok {
		return fmt.Errorf("fts: buffered static filter: static filter does not support serialization")
	}

	if err := f.Build(); err != nil {
		return err
	}

	return serializable.Serialize(w)
}
