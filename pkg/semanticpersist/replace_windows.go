//go:build windows

package semanticpersist

import "errors"

var errAtomicReplaceUnsupported = errors.New("semanticpersist: atomic CURRENT replacement is unsupported on windows")

func atomicReplace(_, _ string) error {
	return errAtomicReplaceUnsupported
}
