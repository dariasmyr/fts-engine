package semanticformat

import "crypto/sha256"

const headVersion = uint16(1)

const headEncodedSize = wireHeaderSize + wireUint64Size + sha256.Size + wireChecksumSize

// Head is the decoded SCUR payload stored in CURRENT.
type Head struct {
	GenerationID uint64
	ManifestHash [sha256.Size]byte
}

func EncodeHead(value Head, limits FileLimits) ([]byte, FileRef, error) {
	if err := limits.validateFile(); err != nil {
		return nil, FileRef{}, err
	}
	if value.GenerationID == 0 {
		return nil, FileRef{}, ErrCorrupt
	}

	e := newEncoder("SCUR", headVersion, headEncodedSize, limits)
	e.writeUint64(value.GenerationID)
	e.writeBytes(value.ManifestHash[:])
	return e.finish()
}

func DecodeHead(data []byte, limits FileLimits) (Head, error) {
	if err := limits.validateFile(); err != nil {
		return Head{}, err
	}

	d, err := newDecoder(data, "SCUR", headVersion, limits)
	if err != nil {
		return Head{}, err
	}

	value := Head{GenerationID: d.readUint64()}
	copy(value.ManifestHash[:], d.readBytes(sha256.Size))
	if err := d.done(); err != nil {
		return Head{}, err
	}
	if value.GenerationID == 0 {
		return Head{}, ErrCorrupt
	}
	return value, nil
}
