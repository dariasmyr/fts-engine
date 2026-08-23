//go:build linux || darwin || freebsd

package semanticpersist

import (
	"errors"
	"os"
	"syscall"
)

type storeLock struct {
	file *os.File
}

func acquireStoreLock(path string) (*storeLock, error) {
	return acquireStoreLockMode(path, syscall.LOCK_EX, true)
}

func acquireStoreReadLock(path string) (*storeLock, error) {
	return acquireStoreLockMode(path, syscall.LOCK_SH, false)
}

func acquireStoreLockMode(path string, mode int, create bool) (*storeLock, error) {
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrSymlink
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	file, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), mode|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrStoreLocked
		}
		return nil, err
	}
	return &storeLock{file: file}, nil
}

func (l *storeLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	file := l.file
	l.file = nil
	unlockErr := syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	closeErr := file.Close()
	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}
