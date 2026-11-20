package storage

import "errors"

var (
	ErrLocked          = errors.New("semanticpersist: persistence operation is locked")
	ErrLockUnsupported = errors.New("semanticpersist: operation locking is unsupported on this platform")
	ErrSymlink         = errors.New("semanticpersist: symlink is not allowed")
)
