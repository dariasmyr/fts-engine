//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package persistfs

import (
	"errors"
	"os"
	"syscall"
)

type FileLock struct{ file *os.File }

func AcquireFileLock(path string, options LockOptions) (*FileLock, bool, error) {
	if options.RejectSymlink {
		if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
			return nil, false, ErrSymlink
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, false, err
		}
	}

	var file *os.File
	var err error
	if options.RejectSymlink {
		flags := os.O_RDWR | syscall.O_NOFOLLOW
		if options.Create {
			flags |= os.O_CREATE
		}
		file, err = os.OpenFile(path, flags, 0o600)
	} else {
		file, err = openPortableLockFile(path, options)
	}
	if err != nil {
		if options.RejectSymlink && errors.Is(err, syscall.ELOOP) {
			return nil, false, ErrSymlink
		}
		return nil, false, err
	}
	if options.RejectSymlink {
		// The pre-open Lstat and O_NOFOLLOW together reject both an existing
		// symlink and a replacement raced in before open.
	}
	operation := syscall.LOCK_EX
	if options.Shared {
		operation = syscall.LOCK_SH
	}
	if options.NonBlocking {
		operation |= syscall.LOCK_NB
	}
	if err := syscall.Flock(int(file.Fd()), operation); err != nil {
		_ = file.Close()
		if options.NonBlocking && (errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN)) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &FileLock{file: file}, true, nil
}

func (l *FileLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	file := l.file
	l.file = nil
	unlockErr := syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	closeErr := file.Close()
	return errors.Join(unlockErr, closeErr)
}
