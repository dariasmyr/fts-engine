package semanticpersist

import "github.com/dariasmyr/fts-engine/pkg/semanticpersist/format"

func encodeCurrent(value currentRecord, limits Limits) ([]byte, fileReference, error) {
	data, ref, err := format.EncodeCurrent(format.Current{GenerationID: value.GenerationID, ManifestHash: value.ManifestHash}, codecLimits(limits))
	return data, persistReference(ref), mapCodecError(err)
}

func decodeCurrent(data []byte, limits Limits) (currentRecord, error) {
	value, err := format.DecodeCurrent(data, codecLimits(limits))
	if err != nil {
		return currentRecord{}, mapCodecError(err)
	}
	return currentRecord{GenerationID: value.GenerationID, ManifestHash: value.ManifestHash}, nil
}
