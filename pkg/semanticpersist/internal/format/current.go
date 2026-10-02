package format

import "crypto/sha256"

const currentVersion = uint16(1)

// Current is the decoded SCUR payload.
type Current struct {
	GenerationID uint64
	ManifestHash [sha256.Size]byte
}

func EncodeCurrent(value Current, limits Limits) ([]byte, FileReference, error) {
	if value.GenerationID == 0 {
		return nil, FileReference{}, ErrCorrupt
	}
	e := newEncoder("SCUR", currentVersion, limits)
	e.u64(value.GenerationID)
	e.raw(value.ManifestHash[:])
	return e.finish()
}

func DecodeCurrent(data []byte, limits Limits) (Current, error) {
	d, err := newDecoder(data, "SCUR", currentVersion, limits)
	if err != nil {
		return Current{}, err
	}
	value := Current{GenerationID: d.u64()}
	copy(value.ManifestHash[:], d.take(sha256.Size))
	if err := d.done(); err != nil {
		return Current{}, err
	}
	if value.GenerationID == 0 {
		return Current{}, ErrCorrupt
	}
	return value, nil
}
