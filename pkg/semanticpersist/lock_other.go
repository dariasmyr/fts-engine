//go:build !linux && !darwin && !freebsd

package semanticpersist

type storeLock struct{}

func acquireStoreLock(string) (*storeLock, error) { return nil, ErrLockUnsupported }

func (*storeLock) Close() error { return nil }
