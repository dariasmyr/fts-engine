package generationformat

import (
	"crypto/sha256"

	"github.com/dariasmyr/fts-engine/v2/internal/format"
)

// FileRef identifies an immutable persisted file by size and SHA-256.
type FileRef = format.FileRef

func encodeFileRef(e *encoder, value FileRef) {
	e.writeUint64(value.Size)
	e.writeBytes(value.SHA256[:])
}

func decodeFileRef(d *decoder) FileRef {
	value := FileRef{Size: d.readUint64()}
	copy(value.SHA256[:], d.readBytes(sha256.Size))
	return value
}
