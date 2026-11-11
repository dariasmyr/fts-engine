package generationformat

import (
	"crypto/sha256"
	"math"
	"unicode/utf8"
)

const manifestVersion = uint16(6)

const (
	fileReferenceEncodedSize = wireUint64Size + sha256.Size
	manifestFixedEncodedSize = wireHeaderSize + wireUint64Size + wireUint32Size + fileReferenceEncodedSize + wireChecksumSize
	manifestSegmentFixedSize = wireStringLengthPrefixSize + 2*fileReferenceEncodedSize
)

// SegmentRef references the immutable files that make up one persisted segment.
type SegmentRef struct {
	ObjectID string
	Vectors  FileRef
	Graph    FileRef
}

// GenerationManifest is the decoded SMAN payload.
type GenerationManifest struct {
	GenerationID uint64
	Segments     []SegmentRef
	State        FileRef
}

func EncodeManifest(value GenerationManifest, limits Config) ([]byte, FileRef, error) {
	if err := limits.validateManifest(); err != nil {
		return nil, FileRef{}, err
	}
	if value.GenerationID == 0 || !value.State.Valid() {
		return nil, FileRef{}, ErrCorrupt
	}
	if len(value.Segments) > limits.MaxSegments || uint64(len(value.Segments)) > math.MaxUint32 {
		return nil, FileRef{}, ErrLimitExceeded
	}

	expectedSize := uint64(manifestFixedEncodedSize)
	for i, segment := range value.Segments {
		if segment.ObjectID == "" || !utf8.ValidString(segment.ObjectID) {
			return nil, FileRef{}, codecErrorf(ErrCorrupt, "manifest segment %d has invalid object ID", i)
		}
		if len(segment.ObjectID) > limits.MaxStringBytes {
			return nil, FileRef{}, ErrLimitExceeded
		}
		if !segment.Vectors.Valid() || !segment.Graph.Valid() {
			return nil, FileRef{}, codecErrorf(ErrCorrupt, "manifest segment %d has invalid file reference", i)
		}
		if !accumulateEncodedSize(&expectedSize, manifestSegmentFixedSize) ||
			!accumulateEncodedSize(&expectedSize, uint64(len(segment.ObjectID))) {
			return nil, FileRef{}, ErrLimitExceeded
		}
	}

	e := newEncoder("SMAN", manifestVersion, expectedSize, limits)
	e.writeUint64(value.GenerationID)
	e.writeUint32(uint32(len(value.Segments)))
	for _, segment := range value.Segments {
		e.writeString(segment.ObjectID, limits.MaxStringBytes)
		encodeFileRef(e, segment.Vectors)
		encodeFileRef(e, segment.Graph)
	}
	encodeFileRef(e, value.State)
	return e.finish()
}

func DecodeManifest(data []byte, limits Config) (GenerationManifest, error) {
	if err := limits.validateManifest(); err != nil {
		return GenerationManifest{}, err
	}

	d, err := newDecoder(data, "SMAN", manifestVersion, limits)
	if err != nil {
		return GenerationManifest{}, err
	}

	value := GenerationManifest{GenerationID: d.readUint64()}
	countValue := uint64(d.readUint32())
	if countValue > uint64(limits.MaxSegments) {
		return GenerationManifest{}, ErrLimitExceeded
	}

	remaining := d.remaining() - fileReferenceEncodedSize
	if remaining < 0 || countValue > uint64(remaining/manifestSegmentFixedSize) {
		return GenerationManifest{}, ErrCorrupt
	}

	value.Segments = make([]SegmentRef, int(countValue))
	for i := range value.Segments {
		segment := SegmentRef{
			ObjectID: d.readString(),
			Vectors:  decodeFileRef(d),
			Graph:    decodeFileRef(d),
		}
		if err := d.err(); err != nil {
			return GenerationManifest{}, err
		}
		if segment.ObjectID == "" || !segment.Vectors.Valid() || !segment.Graph.Valid() {
			return GenerationManifest{}, ErrCorrupt
		}
		value.Segments[i] = segment
	}

	value.State = decodeFileRef(d)
	if err := d.done(); err != nil {
		return GenerationManifest{}, err
	}
	if value.GenerationID == 0 || !value.State.Valid() {
		return GenerationManifest{}, ErrCorrupt
	}
	return value, nil
}
