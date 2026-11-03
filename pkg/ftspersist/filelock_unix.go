//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package ftspersist

import (
	"errors"
	"os"
	"syscall"
)

type fileLock struct {
	file *os.File
}

func acquireFileLock(path string, shared, nonblocking bool) (*fileLock, bool, error) {
	var file *os.File
	var err error
	if shared {
		file, err = os.Open(path)
		if errors.Is(err, os.ErrNotExist) {
			file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
		}
	} else {
		file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	}
	if err != nil {
		return nil, false, err
	}
	operation := syscall.LOCK_EX
	if shared {
		operation = syscall.LOCK_SH
	}
	if nonblocking {
		operation |= syscall.LOCK_NB
	}
	if err := syscall.Flock(int(file.Fd()), operation); err != nil {
		_ = file.Close()
		if nonblocking && (errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN)) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &fileLock{file: file}, true, nil
}

func (l *fileLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	return errors.Join(unlockErr, closeErr)
}
