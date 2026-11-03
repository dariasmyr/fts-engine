package ftspersist

import (
	"fmt"
	"sync"
)

// onceClose makes resource cleanup idempotent and safe for concurrent callers.
// The wrapped function runs once; every call returns the same cleanup error.
func onceClose(close func() error) func() error {
	var once sync.Once
	var closeErr error
	return func() error {
		once.Do(func() {
			if close != nil {
				closeErr = close()
			}
		})
		return closeErr
	}
}

func wrapError(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf(format+": %w", append(args, err)...)
}
