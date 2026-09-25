//go:build !linux && !darwin && !freebsd

package semanticpersist

import "errors"

var errStoreLockUnsupported = errors.New("semanticpersist: writable store locking is unsupported on this platform")

type storeLock struct{}

func acquireStoreLock(string) (*storeLock, error)     { return nil, errStoreLockUnsupported }
func acquireStoreReadLock(string) (*storeLock, error) { return nil, errStoreLockUnsupported }

func (*storeLock) Close() error { return nil }
