// Package persist contains storage primitives shared by index persistence
// implementations. It does not define lexical or semantic payload formats.
package persist

import "crypto/sha256"

// Reference identifies an immutable persisted file by size and SHA-256.
type Reference struct {
	Size   uint64
	SHA256 [sha256.Size]byte
}

// Hash returns the SHA-256 identity of data.
func Hash(data []byte) [sha256.Size]byte {
	return sha256.Sum256(data)
}
