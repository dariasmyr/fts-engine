// Package persistfs provides low-level filesystem mechanics shared by the
// repository's persistence packages. Publication layouts and recovery policy
// belong to their respective persistence packages.
//
// Advisory locks use flock on supported Unix systems and LockFileEx on
// Windows. Other platforms retain ftspersist's historical best-effort behavior
// of opening the lock file without an operating-system lock. Callers requiring
// mandatory advisory locks must reject unsupported platforms themselves.
package persistfs

import "errors"

var (
	ErrLimitExceeded = errors.New("persistfs: file size limit exceeded")
	ErrNotDirectory  = errors.New("persistfs: path is not a directory")
	ErrNotRegular    = errors.New("persistfs: path is not a regular file")
	ErrSymlink       = errors.New("persistfs: symlink is not allowed")
)
