package semanticpersist

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
)

const (
	manifestMagic   = "SMAN"
	manifestVersion = uint16(1)
	currentMagic    = "SCUR"
	currentVersion  = uint16(1)
)

func encodeManifest(value manifest, limits Limits) ([]byte, fileReference, error) {
	if value.GenerationID == 0 || !validObjectID(value.ObjectID) {
		return nil, fileReference{}, ErrCorrupt
	}
	e := newEncoder(manifestMagic, manifestVersion, limits.MaxFileBytes)
	e.u64(value.GenerationID)
	e.string(value.ObjectID, limits.MaxStringBytes)
	encodeFileReference(e, value.Vectors)
	encodeFileReference(e, value.SegmentMeta)
	encodeFileReference(e, value.State)
	return e.finish()
}

func decodeManifest(data []byte, limits Limits) (manifest, error) {
	d, err := newDecoder(data, manifestMagic, manifestVersion, limits)
	if err != nil {
		return manifest{}, err
	}
	value := manifest{GenerationID: d.u64(), ObjectID: d.string(), Vectors: decodeFileReference(d), SegmentMeta: decodeFileReference(d), State: decodeFileReference(d)}
	if err := d.done(); err != nil {
		return manifest{}, err
	}
	if value.GenerationID == 0 || !validObjectID(value.ObjectID) {
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

func objectID(vectorsHash, metaHash [sha256.Size]byte) string {
	hash := sha256.New()
	_, _ = hash.Write(vectorsHash[:])
	_, _ = hash.Write(metaHash[:])
	return "seg-" + hex.EncodeToString(hash.Sum(nil))
}

func validObjectID(id string) bool {
	if len(id) != len("seg-")+sha256.Size*2 || !strings.HasPrefix(id, "seg-") || filepath.IsAbs(id) || filepath.VolumeName(id) != "" || strings.ContainsAny(id, `/\`) || id == "." || id == ".." {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(id, "seg-"))
	return err == nil && len(decoded) == sha256.Size && id == strings.ToLower(id)
}
