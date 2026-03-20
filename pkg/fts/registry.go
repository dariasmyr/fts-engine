package fts

import (
	"fmt"
	"sync"
)

type IndexFactory func() (Index, error)
type FilterFactory func() (Filter, error)

type registry[T any, F ~func() (T, error)] struct {
	mu   sync.RWMutex
	data map[string]F
}

func newRegistry[T any, F ~func() (T, error)]() registry[T, F] {
	return registry[T, F]{
		data: make(map[string]F),
	}
}

func (r *registry[T, F]) register(name string, factory F, kind string) error {
	if name == "" {
		return fmt.Errorf("fts: register %s: empty name", kind)
	}
	if factory == nil {
		return fmt.Errorf("fts: register %s: nil factory", kind)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.data[name]; exists {
		return fmt.Errorf("fts: register %s: duplicate name %q", kind, name)
	}

	r.data[name] = factory
	return nil
}

func (r *registry[T, F]) new(name string, kind string) (T, error) {
	r.mu.RLock()
	factory, ok := r.data[name]
	r.mu.RUnlock()
	if !ok {
		var zero T
		return zero, fmt.Errorf("fts: unknown %s %q", kind, name)
	}

	return factory()
}

var (
	indexRegistry  = newRegistry[Index, IndexFactory]()
	filterRegistry = newRegistry[Filter, FilterFactory]()
)

func RegisterIndex(name string, factory IndexFactory) error {
	return indexRegistry.register(name, factory, "index")
}

func NewIndex(name string) (Index, error) {
	return indexRegistry.new(name, "index")
}

func RegisterFilter(name string, factory FilterFactory) error {
	return filterRegistry.register(name, factory, "filter")
}

func NewFilter(name string) (Filter, error) {
	return filterRegistry.new(name, "filter")
}
