package semanticpersist

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"path/filepath"
	"strings"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	semanticformat "github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/format"
)

const (
	manifestMagic   = "SMAN"
	manifestVersion = uint16(4)
)

func encodeManifest(value manifest, limits Limits) ([]byte, fileReference, error) {
	data, ref, err := semanticformat.EncodeManifest(semanticformat.Manifest{Version: value.Version, GenerationID: value.GenerationID, ObjectID: value.ObjectID, SegmentKind: value.SegmentKind, Vectors: formatReference(value.Vectors), Graph: formatReference(value.Graph), State: formatReference(value.State)}, codecLimits(limits))
	return data, persistReference(ref), mapCodecError(err)
}

func decodeManifest(data []byte, limits Limits) (manifest, error) {
	value, err := semanticformat.DecodeManifest(data, codecLimits(limits))
	if err != nil {
		return manifest{}, mapCodecError(err)
	}
	return manifest{Version: value.Version, GenerationID: value.GenerationID, ObjectID: value.ObjectID, SegmentKind: value.SegmentKind, Vectors: persistReference(value.Vectors), Graph: persistReference(value.Graph), State: persistReference(value.State)}, nil
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
	return value.SegmentKind == semantic.SegmentKindChunkHNSW && value.Vectors.Size != 0 && value.Graph.Size != 0 && value.State.Size != 0 && value.Vectors.SHA256 != [sha256.Size]byte{} && value.Graph.SHA256 != [sha256.Size]byte{} && value.State.SHA256 != [sha256.Size]byte{}
}
func validObjectID(id string) bool {
	if len(id) != len("seg-")+sha256.Size*2 || !strings.HasPrefix(id, "seg-") || filepath.IsAbs(id) || filepath.VolumeName(id) != "" || strings.ContainsAny(id, `/\`) || id == "." || id == ".." {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(id, "seg-"))
	return err == nil && len(decoded) == sha256.Size && id == strings.ToLower(id)
}
