package hnswformat

// VectorLimits bounds the SVEC vector payload format.
type VectorLimits struct {
	MaxDimensions  int
	MaxVectors     int
	MaxVectorBytes uint64
	MaxK           int
}

func (l VectorLimits) validate() error {
	if l.MaxDimensions <= 0 || l.MaxVectors <= 0 || l.MaxVectorBytes == 0 || l.MaxK <= 0 {
		return ErrLimitExceeded
	}
	return nil
}

// SegmentLimits bounds decoding and validation of one immutable HNSW segment.
type SegmentLimits struct {
	MaxFileBytes   uint64
	MaxGraphBytes  uint64
	MaxVectorBytes uint64
	MaxVectors     int
	MaxGraphLinks  uint64
	MaxDimensions  int
}
