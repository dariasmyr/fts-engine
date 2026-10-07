//go:build !linux && !darwin && !freebsd

package semanticpersist

type serviceLock struct{}

func acquireStoreLock(string) (*serviceLock, error) { return nil, ErrLockUnsupported }

func (*serviceLock) Close() error { return nil }
