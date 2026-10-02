package format

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
)

const manifestVersion = uint16(6)

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
	e := newEncoder("SMAN", manifestVersion, limits)
	e.u64(value.GenerationID)
	e.u32(uint32(len(value.Segments)))
	for _, segment := range value.Segments {
		if !validObjectID(segment.ObjectID) || !validSegment(segment) {
			return nil, FileReference{}, ErrCorrupt
		}
		e.string(segment.ObjectID, limits.MaxStringBytes)
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
	value := Manifest{GenerationID: d.u64()}
	countValue := uint64(d.u32())
	if countValue > uint64(limits.MaxVectors) || countValue > uint64(d.remaining()/152) {
		return Manifest{}, ErrLimitExceeded
	}
	count := int(countValue)
	value.Segments = make([]SegmentObject, count)
	for i := range value.Segments {
		segment := SegmentObject{ObjectID: d.string()}
		segment.Vectors, segment.Graph = decodeFileReference(d), decodeFileReference(d)
		if !validObjectID(segment.ObjectID) || !validSegment(segment) {
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

func encodeFileReference(e *encoder, value FileReference) { e.u64(value.Size); e.raw(value.SHA256[:]) }
func decodeFileReference(d *decoder) FileReference {
	value := FileReference{Size: d.u64()}
	copy(value.SHA256[:], d.take(sha256.Size))
	return value
}

func validReference(ref FileReference) bool {
	return ref.Size != 0 && ref.SHA256 != [sha256.Size]byte{}
}

func validSegment(value SegmentObject) bool {
	return validReference(value.Vectors) && validReference(value.Graph)
}

func validObjectID(id string) bool {
	if len(id) != len("seg-")+sha256.Size*2 || !strings.HasPrefix(id, "seg-") || filepath.IsAbs(id) || filepath.VolumeName(id) != "" || strings.ContainsAny(id, `/\`) || id == "." || id == ".." {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(id, "seg-"))
	return err == nil && len(decoded) == sha256.Size && id == strings.ToLower(id)
}
