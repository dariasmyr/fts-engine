package fts

import "errors"

// ErrPersistenceRebuildRequired indicates that persisted data is valid enough
// to identify as legacy but cannot be migrated safely.
var ErrPersistenceRebuildRequired = errors.New("rebuild persisted data")
