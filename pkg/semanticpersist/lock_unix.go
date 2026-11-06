//go:build linux || darwin || freebsd

package semanticpersist

import (
	"errors"

	"github.com/dariasmyr/fts-engine/v2/internal/persistfs"
)

type serviceLock = persistfs.FileLock

func acquireStoreLock(path string) (*serviceLock, error) {
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
		return nil, ErrStoreLocked
	}
	return lock, nil
}
