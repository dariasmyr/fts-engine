package semanticpersist

import (
	"errors"

	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/storage"
)

type operationLayoutMode uint8

const (
	operationLayoutExisting operationLayoutMode = iota
	operationLayoutPrepare
)

type storageOperation struct {
	layout             layout
	rootCreated        bool
	publishedToStorage bool
}

func (o *storageOperation) markPublishedToStorage() { o.publishedToStorage = true }

func (o *storageOperation) mapCloseError(err error) error {
	if o.publishedToStorage {
		return indeterminateError(err)
	}
	return err
}

func withStorageOperation(
	root string,
	mode operationLayoutMode,
	run func(*storageOperation) error,
) error {
	if root == "" || run == nil {
		return ErrCorrupt
	}
	_, err := withStorageOperationResult(root, mode, func(operation *storageOperation) (struct{}, error) {
		return struct{}{}, run(operation)
	})
	return err
}

func withStorageOperationResult[T any](
	root string,
	mode operationLayoutMode,
	run func(*storageOperation) (T, error),
) (result T, err error) {
	if root == "" || run == nil {
		return result, ErrCorrupt
	}

	l := newLayout(root)
	rootCreated := false
	switch mode {
	case operationLayoutExisting:
		err = l.validate()
	case operationLayoutPrepare:
		rootCreated, err = l.prepare()
	default:
		err = ErrCorrupt
	}
	if err != nil {
		return result, err
	}

	lock, err := storage.AcquireOperationLock(l.lock)
	if err != nil {
		return result, err
	}
	operation := &storageOperation{layout: l, rootCreated: rootCreated}
	defer func() {
		err = errors.Join(err, operation.mapCloseError(lock.Close()))
	}()

	if err := l.validate(); err != nil {
		return result, err
	}
	return run(operation)
}
