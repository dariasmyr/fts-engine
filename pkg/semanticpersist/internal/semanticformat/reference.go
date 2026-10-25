package semanticformat

import "crypto/sha256"

// FileRef identifies an immutable persisted file by size and SHA-256.
type FileRef struct {
	Size   uint64
	SHA256 [sha256.Size]byte
}

func encodeFileRef(e *encoder, value FileRef) {
	e.writeUint64(value.Size)
	e.writeBytes(value.SHA256[:])
}

func decodeFileRef(d *decoder) FileRef {
	value := FileRef{Size: d.readUint64()}
	copy(value.SHA256[:], d.readBytes(sha256.Size))
	return value
}

func validFileRef(ref FileRef) bool {
	return ref.Size != 0 && ref.SHA256 != [sha256.Size]byte{}
}
