package generationformat

import "crypto/sha256"

const headVersion = uint16(1)

const headEncodedSize = wireHeaderSize + wireUint64Size + sha256.Size + wireChecksumSize

// Head is the decoded SCUR payload stored in CURRENT.
type Head struct {
	GenerationID uint64
	ManifestHash [sha256.Size]byte
}

func EncodeHead(value Head, limits Config) ([]byte, error) {
	if err := limits.validateFile(); err != nil {
		return nil, err
	}
	if value.GenerationID == 0 {
		return nil, ErrCorrupt
	}

	e := newEncoder("SCUR", headVersion, headEncodedSize, limits)
	e.writeUint64(value.GenerationID)
	e.writeBytes(value.ManifestHash[:])
	data, _, err := e.finish()
	return data, err
}

func DecodeHead(data []byte, limits Config) (Head, error) {
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
