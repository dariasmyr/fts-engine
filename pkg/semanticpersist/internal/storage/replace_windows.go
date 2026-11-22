//go:build windows

package storage

import "errors"

var errAtomicReplaceUnsupported = errors.New("semanticpersist: atomic CURRENT replacement is unsupported on windows")

func AtomicReplace(_, _ string) error {
	return errAtomicReplaceUnsupported
}
