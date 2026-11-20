package semanticpersist

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/storage"
)

func TestWithStorageOperationPreparesValidatesAndReleases(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "store")
	result, err := withStorageOperationResult(root, operationLayoutPrepare, func(operation *storageOperation) (int, error) {
		if !operation.rootCreated {
			t.Fatal("rootCreated = false on first prepare")
		}
		if err := operation.layout.validate(); err != nil {
			t.Fatalf("prepared layout validation: %v", err)
		}
		return 42, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != 42 {
		t.Fatalf("result = %d, want 42", result)
	}

	callbackErr := errors.New("callback failed")
	err = withStorageOperation(root, operationLayoutExisting, func(operation *storageOperation) error {
		if operation.rootCreated {
			t.Fatal("rootCreated = true for existing layout")
		}
		return callbackErr
	})
	if !errors.Is(err, callbackErr) {
		t.Fatalf("operation error = %v, want callback error", err)
	}

	if err := withStorageOperation(root, operationLayoutExisting, func(*storageOperation) error { return nil }); err != nil {
		t.Fatalf("operation after callback error: %v", err)
	}
}

func TestWithStorageOperationRejectsLockedStore(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "store")
	if err := withStorageOperation(root, operationLayoutPrepare, func(*storageOperation) error { return nil }); err != nil {
		t.Fatal(err)
	}
	lock, err := storage.AcquireOperationLock(newLayout(root).lock)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()

	err = withStorageOperation(root, operationLayoutExisting, func(*storageOperation) error {
		t.Fatal("callback ran while store was locked")
		return nil
	})
	if !errors.Is(err, storage.ErrLocked) {
		t.Fatalf("operation error = %v, want ErrLocked", err)
	}
}

func TestWithStorageOperationRejectsInvalidSetup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		root string
		mode operationLayoutMode
		run  func(*storageOperation) error
	}{
		{name: "empty root", mode: operationLayoutPrepare, run: func(*storageOperation) error { return nil }},
		{name: "nil callback", root: t.TempDir(), mode: operationLayoutPrepare},
		{name: "invalid mode", root: t.TempDir(), mode: operationLayoutMode(255), run: func(*storageOperation) error { return nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := withStorageOperation(tt.root, tt.mode, tt.run); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("operation error = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestStorageOperationMapsCloseErrorAfterStoragePublish(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("close failed")
	operation := &storageOperation{}
	if err := operation.mapCloseError(closeErr); !errors.Is(err, closeErr) {
		t.Fatalf("close error before publish = %v, want original error", err)
	}
	operation.markPublishedToStorage()
	if err := operation.mapCloseError(closeErr); !errors.Is(err, ErrIndeterminate) {
		t.Fatalf("close error after publish = %v, want ErrIndeterminate", err)
	}
}
