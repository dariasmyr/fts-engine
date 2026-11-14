//go:build linux || darwin || freebsd

package storage

import (
	"errors"

	"github.com/dariasmyr/fts-engine/v2/internal/persistfs"
)

type OperationLock = persistfs.FileLock

func AcquireOperationLock(path string) (*OperationLock, error) {
	lock, acquired, err := persistfs.AcquireFileLock(path, persistfs.LockOptions{
		NonBlocking: true, Create: true, RejectSymlink: true,
	})
	if errors.Is(err, persistfs.ErrSymlink) {
		return nil, ErrSymlink
	}
	if err != nil {
		return nil, err
	}
	if !acquired {
		return nil, ErrLocked
	}
	return lock, nil
}
