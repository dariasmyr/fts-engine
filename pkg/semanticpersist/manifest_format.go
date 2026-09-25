package semanticpersist

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"path/filepath"
	"strings"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
)

const (
	manifestMagic   = "SMAN"
	manifestVersion = uint16(4)
	currentMagic    = "SCUR"
	currentVersion  = uint16(1)
)

func encodeManifest(value manifest, limits Limits) ([]byte, fileReference, error) {
	if value.GenerationID == 0 || !validObjectID(value.ObjectID) || !validManifestSegment(value) {
		return nil, fileReference{}, ErrCorrupt
	}
	e := newEncoder(manifestMagic, manifestVersion, limits.MaxFileBytes)
	e.u64(value.GenerationID)
	e.string(value.ObjectID, limits.MaxStringBytes)
	e.u8(uint8(value.SegmentKind))
	e.raw(make([]byte, 7))
	encodeFileReference(e, value.Vectors)
	encodeFileReference(e, value.Graph)
	encodeFileReference(e, value.State)
	return e.finish()
}

func decodeManifest(data []byte, limits Limits) (manifest, error) {
	d, err := newDecoder(data, manifestMagic, manifestVersion, limits)
	if err != nil {
		return manifest{}, err
	}
	value := manifest{Version: manifestVersion, GenerationID: d.u64(), ObjectID: d.string(), SegmentKind: semantic.SegmentKind(d.u8())}
	if padding := d.take(7); padding == nil || !allZeroBytes(padding) {
		return manifest{}, ErrCorrupt
	}
	value.Vectors = decodeFileReference(d)
	value.Graph = decodeFileReference(d)
	value.State = decodeFileReference(d)
	if err := d.done(); err != nil {
		return manifest{}, err
	}
	if value.GenerationID == 0 || !validObjectID(value.ObjectID) || !validManifestSegment(value) {
		return manifest{}, ErrCorrupt
	}
	return value, nil
}

func encodeCurrent(value currentRecord, limits Limits) ([]byte, fileReference, error) {
	if value.GenerationID == 0 {
		return nil, fileReference{}, ErrCorrupt
	}
	e := newEncoder(currentMagic, currentVersion, limits.MaxFileBytes)
	e.u64(value.GenerationID)
	e.raw(value.ManifestHash[:])
	return e.finish()
}

func decodeCurrent(data []byte, limits Limits) (currentRecord, error) {
	d, err := newDecoder(data, currentMagic, currentVersion, limits)
	if err != nil {
		return currentRecord{}, err
	}
	value := currentRecord{GenerationID: d.u64()}
	copy(value.ManifestHash[:], d.take(sha256.Size))
	if err := d.done(); err != nil {
		return currentRecord{}, err
	}
	if value.GenerationID == 0 {
		return currentRecord{}, ErrCorrupt
	}
	return value, nil
}

func encodeFileReference(e *encoder, value fileReference) {
	e.u64(value.Size)
	e.raw(value.SHA256[:])
}

func decodeFileReference(d *decoder) fileReference {
	value := fileReference{Size: d.u64()}
	copy(value.SHA256[:], d.take(sha256.Size))
	return value
}

func segmentObjectID(kind semantic.SegmentKind, vectors, graph fileReference) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("semantic-segment-v4"))
	_, _ = hash.Write([]byte{byte(kind)})
	var size [8]byte
	for _, reference := range []fileReference{vectors, graph} {
		binary.LittleEndian.PutUint64(size[:], reference.Size)
		_, _ = hash.Write(size[:])
		_, _ = hash.Write(reference.SHA256[:])
	}
	return "seg-" + hex.EncodeToString(hash.Sum(nil))
}

func validManifestSegment(value manifest) bool {
	validReference := func(reference fileReference) bool {
		return reference.Size != 0 && reference.SHA256 != [sha256.Size]byte{}
	}
	if !validReference(value.Vectors) || !validReference(value.Graph) || !validReference(value.State) {
		return false
	}
	return value.SegmentKind == semantic.SegmentKindChunkHNSW && validReference(value.Graph)
}

func allZeroBytes(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}

func validObjectID(id string) bool {
	if len(id) != len("seg-")+sha256.Size*2 || !strings.HasPrefix(id, "seg-") || filepath.IsAbs(id) || filepath.VolumeName(id) != "" || strings.ContainsAny(id, `/\`) || id == "." || id == ".." {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(id, "seg-"))
	return err == nil && len(decoded) == sha256.Size && id == strings.ToLower(id)
}
