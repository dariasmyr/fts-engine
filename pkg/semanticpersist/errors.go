package semanticpersist

import "errors"

var (
	ErrCorrupt            = errors.New("semanticpersist: corrupt data")
	ErrUnsupportedVersion = errors.New("semanticpersist: unsupported version")
	ErrLimitExceeded      = errors.New("semanticpersist: configured limit exceeded")
	ErrCurrentMissing     = errors.New("semanticpersist: CURRENT is missing")
	ErrPathEscape         = errors.New("semanticpersist: path escapes store root")
	ErrSymlink            = errors.New("semanticpersist: symlink is not allowed")
	ErrGenerationExists   = errors.New("semanticpersist: generation already exists")
	ErrIndeterminate      = errors.New("semanticpersist: publication outcome is indeterminate")
	ErrStoreLocked        = errors.New("semanticpersist: store is locked by another writer")
	ErrStoreClosed        = errors.New("semanticpersist: store is closed")
	ErrLockUnsupported    = errors.New("semanticpersist: writable store locking is unsupported on this platform")
	ErrStaleGeneration    = errors.New("semanticpersist: expected generation does not match CURRENT")
	ErrEmbeddingMismatch  = errors.New("semanticpersist: embedding descriptor mismatch")
	ErrChunkingMismatch   = errors.New("semanticpersist: chunking descriptor mismatch")
)
