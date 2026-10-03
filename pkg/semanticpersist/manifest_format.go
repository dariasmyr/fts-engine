package semanticpersist

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"path/filepath"
	"strings"

	semanticformat "github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/format"
)

const segmentObjectIdentityDomain = "semantic-segment-object-v1"

func encodeManifest(value manifest, limits Limits) ([]byte, fileReference, error) {
	segments := make([]semanticformat.SegmentObject, len(value.Segments))
	for i, segment := range value.Segments {
		segments[i] = semanticformat.SegmentObject{ObjectID: segment.ObjectID, Vectors: formatReference(segment.Vectors), Graph: formatReference(segment.Graph)}
	}
	data, ref, err := semanticformat.EncodeManifest(semanticformat.Manifest{GenerationID: value.GenerationID, Segments: segments, State: formatReference(value.State)}, codecLimits(limits))
	return data, persistReference(ref), mapCodecError(err)
}

func decodeManifest(data []byte, limits Limits) (manifest, error) {
	value, err := semanticformat.DecodeManifest(data, codecLimits(limits))
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

func validManifestSegment(value manifestSegment) bool {
	return value.Vectors.Size != 0 && value.Graph.Size != 0 && value.Vectors.SHA256 != [sha256.Size]byte{} && value.Graph.SHA256 != [sha256.Size]byte{}
}
func validObjectID(id string) bool {
	if len(id) != len("seg-")+sha256.Size*2 || !strings.HasPrefix(id, "seg-") || filepath.IsAbs(id) || filepath.VolumeName(id) != "" || strings.ContainsAny(id, `/\`) || id == "." || id == ".." {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(id, "seg-"))
	return err == nil && len(decoded) == sha256.Size && id == strings.ToLower(id)
}
