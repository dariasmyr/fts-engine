package hnswformat

import "crypto/sha256"

// FileRef identifies an immutable persisted file by size and SHA-256.
type FileRef struct {
	Size   uint64
	SHA256 [sha256.Size]byte
}
