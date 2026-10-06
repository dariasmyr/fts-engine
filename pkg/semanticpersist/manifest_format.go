package semanticpersist

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/format"
)

const segmentObjectIdentityDomain = "semantic-segment-object-v1"

func encodeManifest(value manifest, limits Limits) ([]byte, fileReference, error) {
	segments := make([]format.SegmentObject, len(value.Segments))
	for i, segment := range value.Segments {
		segments[i] = format.SegmentObject{ObjectID: segment.ObjectID, Vectors: formatReference(segment.Vectors), Graph: formatReference(segment.Graph)}
	}
	data, ref, err := format.EncodeManifest(format.Manifest{GenerationID: value.GenerationID, Segments: segments, State: formatReference(value.State)}, codecLimits(limits))
	return data, persistReference(ref), mapCodecError(err)
}

func decodeManifest(data []byte, limits Limits) (manifest, error) {
	value, err := format.DecodeManifest(data, codecLimits(limits))
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
