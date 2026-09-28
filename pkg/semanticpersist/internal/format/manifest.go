package format

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
)

const manifestVersion = uint16(4)

// Manifest is the decoded SMAN payload.
type Manifest struct {
	Version      uint16
	GenerationID uint64
	ObjectID     string
	SegmentKind  semantic.SegmentKind
	Vectors      FileReference
	Graph        FileReference
	State        FileReference
}

func EncodeManifest(value Manifest, limits Limits) ([]byte, FileReference, error) {
	if value.GenerationID == 0 || !validObjectID(value.ObjectID) || !validSegment(value) {
		return nil, FileReference{}, ErrCorrupt
	}
	e := newEncoder("SMAN", manifestVersion, limits)
	e.u64(value.GenerationID)
	e.string(value.ObjectID, limits.MaxStringBytes)
	e.u8(uint8(value.SegmentKind))
	e.raw(make([]byte, 7))
	encodeFileReference(e, value.Vectors)
	encodeFileReference(e, value.Graph)
	encodeFileReference(e, value.State)
	return e.finish()
}

func DecodeManifest(data []byte, limits Limits) (Manifest, error) {
	d, err := newDecoder(data, "SMAN", manifestVersion, limits)
	if err != nil {
		return Manifest{}, err
	}
	value := Manifest{Version: manifestVersion, GenerationID: d.u64(), ObjectID: d.string(), SegmentKind: semantic.SegmentKind(d.u8())}
	padding := d.take(7)
	for _, b := range padding {
		if b != 0 {
			return Manifest{}, ErrCorrupt
		}
	}
	value.Vectors, value.Graph, value.State = decodeFileReference(d), decodeFileReference(d), decodeFileReference(d)
	if err := d.done(); err != nil {
		return Manifest{}, err
	}
	if value.GenerationID == 0 || !validObjectID(value.ObjectID) || !validSegment(value) {
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

func validSegment(value Manifest) bool {
	valid := func(ref FileReference) bool { return ref.Size != 0 && ref.SHA256 != [sha256.Size]byte{} }
	return valid(value.Vectors) && valid(value.Graph) && valid(value.State) && value.SegmentKind == semantic.SegmentKindChunkHNSW
}

func validObjectID(id string) bool {
	if len(id) != len("seg-")+sha256.Size*2 || !strings.HasPrefix(id, "seg-") || filepath.IsAbs(id) || filepath.VolumeName(id) != "" || strings.ContainsAny(id, `/\`) || id == "." || id == ".." {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(id, "seg-"))
	return err == nil && len(decoded) == sha256.Size && id == strings.ToLower(id)
}
