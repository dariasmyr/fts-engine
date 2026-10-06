package semanticpersist

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"path/filepath"
	"strings"
)

const manifestVersion = uint16(6) // SMAN wire-format version.

const (
	// File byte size and SHA-256.
	fileReferenceEncodedSize = wireUint64Size + sha256.Size
	// Header, generation, segment count, state reference, and CRC32.
	manifestFixedEncodedSize = wireHeaderSize + wireUint64Size + wireUint32Size + fileReferenceEncodedSize + wireChecksumSize
	// Object-ID length prefix plus vector and graph references; ID bytes are dynamic.
	manifestSegmentFixedSize = wireStringLengthPrefixSize + 2*fileReferenceEncodedSize
)

type SegmentObject struct {
	ObjectID string
	Vectors  FileReference
	Graph    FileReference
}

// Manifest is the decoded SMAN payload.
type Manifest struct {
	GenerationID uint64
	Segments     []SegmentObject
	State        FileReference
}

func EncodeManifest(value Manifest, limits Limits) ([]byte, FileReference, error) {
	if value.GenerationID == 0 || len(value.Segments) > limits.MaxVectors || !validReference(value.State) {
		return nil, FileReference{}, ErrCorrupt
	}
	// The manifest header, generation, segment count, state reference, and
	// checksum are fixed. Each segment adds two fixed-size references plus its
	// dynamically sized object ID.
	expectedSize := uint64(manifestFixedEncodedSize)
	for _, segment := range value.Segments {
		if !ValidObjectID(segment.ObjectID) || !validSegment(segment) {
			return nil, FileReference{}, ErrCorrupt
		}
		if !addEncodedSize(&expectedSize, manifestSegmentFixedSize) || !addEncodedSize(&expectedSize, uint64(len(segment.ObjectID))) {
			return nil, FileReference{}, ErrLimitExceeded
		}
	}
	e := newEncoder("SMAN", manifestVersion, expectedSize, limits)
	e.writeUint64(value.GenerationID)
	e.writeUint32(uint32(len(value.Segments)))
	for _, segment := range value.Segments {
		e.writeString(segment.ObjectID, limits.MaxStringBytes)
		encodeFileReference(e, segment.Vectors)
		encodeFileReference(e, segment.Graph)
	}
	encodeFileReference(e, value.State)
	return e.finish()
}

func DecodeManifest(data []byte, limits Limits) (Manifest, error) {
	d, err := newDecoder(data, "SMAN", manifestVersion, limits)
	if err != nil {
		return Manifest{}, err
	}
	value := Manifest{GenerationID: d.readUint64()}
	countValue := uint64(d.readUint32())
	if countValue > uint64(limits.MaxVectors) || countValue > uint64(d.remaining()/152) {
		return Manifest{}, ErrLimitExceeded
	}
	count := int(countValue)
	value.Segments = make([]SegmentObject, count)
	for i := range value.Segments {
		segment := SegmentObject{ObjectID: d.readString()}
		segment.Vectors, segment.Graph = decodeFileReference(d), decodeFileReference(d)
		if !ValidObjectID(segment.ObjectID) || !validSegment(segment) {
			return Manifest{}, ErrCorrupt
		}
		value.Segments[i] = segment
	}
	value.State = decodeFileReference(d)
	if err := d.done(); err != nil {
		return Manifest{}, err
	}
	if value.GenerationID == 0 || !validReference(value.State) {
		return Manifest{}, ErrCorrupt
	}
	return value, nil
}

func encodeFileReference(e *encoder, value FileReference) {
	e.writeUint64(value.Size)
	e.writeBytes(value.SHA256[:])
}
func decodeFileReference(d *decoder) FileReference {
	value := FileReference{Size: d.readUint64()}
	copy(value.SHA256[:], d.readBytes(sha256.Size))
	return value
}

func validReference(ref FileReference) bool {
	return ref.Size != 0 && ref.SHA256 != [sha256.Size]byte{}
}

func validSegment(value SegmentObject) bool {
	return validReference(value.Vectors) && validReference(value.Graph)
}

func ValidObjectID(id string) bool {
	const prefix = "seg-"
	const hashHexLength = sha256.Size * 2

	if len(id) != len(prefix)+hashHexLength {
		return false
	}

	if !strings.HasPrefix(id, prefix) {
		return false
	}

	if filepath.IsAbs(id) ||
		filepath.VolumeName(id) != "" ||
		strings.ContainsAny(id, `/\`) ||
		id == "." ||
		id == ".." {
		return false
	}

	hashPart := strings.TrimPrefix(id, prefix)

	decoded, err := hex.DecodeString(hashPart)
	if err != nil || len(decoded) != sha256.Size {
		return false
	}

	return id == strings.ToLower(id)
}

const segmentObjectIdentityDomain = "semantic-segment-object-v1"

func encodeManifest(value manifest, limits Limits) ([]byte, fileReference, error) {
	segments := make([]SegmentObject, len(value.Segments))
	for i, segment := range value.Segments {
		segments[i] = SegmentObject{ObjectID: segment.ObjectID, Vectors: formatReference(segment.Vectors), Graph: formatReference(segment.Graph)}
	}
	data, ref, err := EncodeManifest(Manifest{GenerationID: value.GenerationID, Segments: segments, State: formatReference(value.State)}, codecLimits(limits))
	return data, persistReference(ref), mapCodecError(err)
}

func decodeManifest(data []byte, limits Limits) (manifest, error) {
	value, err := DecodeManifest(data, codecLimits(limits))
	if err != nil {
		return manifest{}, mapCodecError(err)
	}
	segments := make([]manifestSegment, len(value.Segments))
	for i, segment := range value.Segments {
		segments[i] = manifestSegment{ObjectID: segment.ObjectID, Vectors: persistReference(segment.Vectors), Graph: persistReference(segment.Graph)}
	}
	return manifest{GenerationID: value.GenerationID, Segments: segments, State: persistReference(value.State)}, nil
}

func segmentObjectID(vectors, graph fileReference) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(segmentObjectIdentityDomain))
	var size [8]byte
	for _, reference := range []fileReference{vectors, graph} {
		binary.LittleEndian.PutUint64(size[:], reference.Size)
		_, _ = hash.Write(size[:])
		_, _ = hash.Write(reference.SHA256[:])
	}
	return "seg-" + hex.EncodeToString(hash.Sum(nil))
}
