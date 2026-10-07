package semanticpersist

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"path/filepath"
	"strings"

	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/semanticformat"
)

const segmentObjectIdentityDomain = "semantic-segment-object-v1"

func segmentObjectID(vectors, graph semanticformat.FileRef) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(segmentObjectIdentityDomain))

	var size [8]byte
	for _, ref := range []semanticformat.FileRef{vectors, graph} {
		binary.LittleEndian.PutUint64(size[:], ref.Size)
		_, _ = hash.Write(size[:])
		_, _ = hash.Write(ref.SHA256[:])
	}

	return "seg-" + hex.EncodeToString(hash.Sum(nil))
}

func validObjectID(id string) bool {
	const prefix = "seg-"
	const hashHexLength = sha256.Size * 2

	if len(id) != len(prefix)+hashHexLength || !strings.HasPrefix(id, prefix) {
		return false
	}
	if filepath.IsAbs(id) || filepath.VolumeName(id) != "" || strings.ContainsAny(id, `/\\`) {
		return false
	}

	decoded, err := hex.DecodeString(strings.TrimPrefix(id, prefix))
	return err == nil && len(decoded) == sha256.Size && id == strings.ToLower(id)
}
