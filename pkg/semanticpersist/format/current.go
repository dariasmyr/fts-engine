package format

import "crypto/sha256"

// SCUR wire-format version.
const currentVersion = uint16(1)

// Current contains only fixed-width fields: the common header, generation ID,
// manifest digest, and checksum footer.
const currentEncodedSize = wireHeaderSize + wireUint64Size + sha256.Size + wireChecksumSize

// Current is the decoded SCUR payload.
type Current struct {
	GenerationID uint64
	ManifestHash [sha256.Size]byte
}

func EncodeCurrent(value Current, limits Limits) ([]byte, FileReference, error) {
	if value.GenerationID == 0 {
		return nil, FileReference{}, ErrCorrupt
	}
	e := newEncoder("SCUR", currentVersion, currentEncodedSize, limits)
	e.writeUint64(value.GenerationID)
	e.writeBytes(value.ManifestHash[:])
	return e.finish()
}

func DecodeCurrent(data []byte, limits Limits) (Current, error) {
	d, err := newDecoder(data, "SCUR", currentVersion, limits)
	if err != nil {
		return Current{}, err
	}
	value := Current{GenerationID: d.readUint64()}
	copy(value.ManifestHash[:], d.readBytes(sha256.Size))
	if err := d.done(); err != nil {
		return Current{}, err
	}
	if value.GenerationID == 0 {
		return Current{}, ErrCorrupt
	}
	return value, nil
}
