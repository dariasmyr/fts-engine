package semanticpersist

import (
	semanticformat "github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/format"
)

const (
	stateMagic   = "SSTA"
	stateVersion = uint16(6)
)

func encodeState(sealed SealedSegment, limits Limits) ([]byte, fileReference, error) {
	if err := validateSealedSegment(sealed, limits); err != nil {
		return nil, fileReference{}, err
	}
	metadata := sealed.Segment.Metadata()
	value := semanticformat.State{Embedding: metadata.Embedding, Chunking: metadata.Chunking, MaxAllocatedVectorID: sealed.MaxAllocatedVectorID, Rows: sealed.Segment.Rows(), ComponentID: sealed.Segment.ComponentID(), MaxK: sealed.MaxK, MaxChunkCandidates: sealed.MaxChunkCandidates, MaxChunksPerDocumentHit: sealed.MaxChunksPerDocumentHit}
	data, ref, err := semanticformat.EncodeState(value, codecLimits(limits))
	return data, persistReference(ref), mapCodecError(err)
}

func decodeState(data []byte, limits Limits) (decodedState, error) {
	value, err := semanticformat.DecodeState(data, codecLimits(limits))
	if err != nil {
		return decodedState{}, mapCodecError(err)
	}
	return decodedState{Embedding: value.Embedding, Chunking: value.Chunking, MaxAllocatedVectorID: value.MaxAllocatedVectorID, ComponentID: value.ComponentID, Rows: value.Rows, MaxK: value.MaxK, MaxChunkCandidates: value.MaxChunkCandidates, MaxChunksPerDocumentHit: value.MaxChunksPerDocumentHit}, nil
}
