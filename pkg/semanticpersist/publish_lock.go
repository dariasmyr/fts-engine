package semanticpersist

import "context"

type publishLock struct{ gate chan struct{} }

func newPublishLock() publishLock { return publishLock{gate: make(chan struct{}, 1)} }
func (l *publishLock) Lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case l.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			l.Unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (l *publishLock) LockUninterruptible() { l.gate <- struct{}{} }
func (l *publishLock) Unlock()              { <-l.gate }
