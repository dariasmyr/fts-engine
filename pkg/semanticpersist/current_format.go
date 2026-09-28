package semanticpersist

import semanticformat "github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/format"

const (
	currentMagic   = "SCUR"
	currentVersion = uint16(1)
)

func encodeCurrent(value currentRecord, limits Limits) ([]byte, fileReference, error) {
	data, ref, err := semanticformat.EncodeCurrent(semanticformat.Current{GenerationID: value.GenerationID, ManifestHash: value.ManifestHash}, codecLimits(limits))
	return data, persistReference(ref), mapCodecError(err)
}

func decodeCurrent(data []byte, limits Limits) (currentRecord, error) {
	value, err := semanticformat.DecodeCurrent(data, codecLimits(limits))
	if err != nil {
		return currentRecord{}, mapCodecError(err)
	}
	return currentRecord{GenerationID: value.GenerationID, ManifestHash: value.ManifestHash}, nil
}
