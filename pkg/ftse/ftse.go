// Package ftse provides the low-level FTSE format for sealed FTS indexes.
// Sealed segments are immutable: built once from an in-memory index, then
// queried by holding the file bytes plus a small term lookup table.
//
// Use pkg/segmentbundle for self-contained stream/blob persistence and
// pkg/ftspersist for the higher-level filesystem persistence workflow.
package ftse

import (
	"errors"
	"fmt"
)

// ErrUnsupportedVersion identifies FTSE versions this reader cannot open.
var ErrUnsupportedVersion = errors.New("ftse: unsupported format version")

// UnsupportedVersionError reports the FTSE version found in an artifact.
type UnsupportedVersionError struct {
	Version uint16
}

func (e *UnsupportedVersionError) Error() string {
	if e.Version == 1 {
		return "ftse: FTSE v1 is no longer supported; rebuild the index as FTSE v3"
	}
	return fmt.Sprintf("ftse: unsupported FTSE version %d", e.Version)
}

func (e *UnsupportedVersionError) Unwrap() error { return ErrUnsupportedVersion }

const (
	magic                = "FTSE"
	legacyFormatVersion  = uint16(2)
	blockedFormatVersion = uint16(3)

	headerLen        = 8
	blockedHeaderLen = 12
	footerLen        = 32
)
