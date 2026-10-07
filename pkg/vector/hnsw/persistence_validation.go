package hnsw

import (
	"crypto/sha256"
	"math"
)

func indexConfigEncodable(index *Index) bool {
	values := [...]int{
		index.Dimensions(),
		index.search.DefaultEfSearch,
		index.search.MaxEfSearch,
		index.search.DefaultVisitLimit,
		index.search.MaxVisitLimit,
		index.search.MaxK,
		index.topology.buildInfo.MaxNeighbors,
		index.topology.buildInfo.LevelZeroMaxNeighbors,
		index.topology.buildInfo.EfConstruction,
	}
	for _, value := range values {
		if value < 0 || uint64(value) > math.MaxUint32 {
			return false
		}
	}
	return true
}

func normalizeGraphLimits(limits GraphLimits) GraphLimits {
	defaults := DefaultGraphLimits()
	if limits.MaxDimensions == 0 {
		limits.MaxDimensions = defaults.MaxDimensions
	}
	if limits.MaxVectors == 0 {
		limits.MaxVectors = defaults.MaxVectors
	}
	if limits.MaxVectorBytes == 0 {
		limits.MaxVectorBytes = defaults.MaxVectorBytes
	}
	if limits.MaxGraphBytes == 0 {
		limits.MaxGraphBytes = defaults.MaxGraphBytes
	}
	if limits.MaxLinks == 0 {
		limits.MaxLinks = defaults.MaxLinks
	}
	if limits.MaxLevel == 0 {
		limits.MaxLevel = defaults.MaxLevel
	}
	if limits.MaxEfSearch == 0 {
		limits.MaxEfSearch = defaults.MaxEfSearch
	}
	if limits.MaxVisitLimit == 0 {
		limits.MaxVisitLimit = defaults.MaxVisitLimit
	}
	if limits.MaxK == 0 {
		limits.MaxK = defaults.MaxK
	}
	if limits.MaxNeighbors == 0 {
		limits.MaxNeighbors = defaults.MaxNeighbors
	}
	if limits.MaxEfConstruction == 0 {
		limits.MaxEfConstruction = defaults.MaxEfConstruction
	}
	return limits
}

func validateGraphLimits(limits GraphLimits) error {
	if limits.MaxDimensions <= 0 || limits.MaxVectors <= 0 || limits.MaxVectorBytes == 0 ||
		limits.MaxGraphBytes < graphFormatHeaderSize+graphFormatFooterSize || limits.MaxLinks == 0 ||
		limits.MaxLevel < 0 || limits.MaxLevel > MaxLevel || limits.MaxEfSearch <= 0 ||
		limits.MaxVisitLimit <= 0 || limits.MaxK <= 0 || limits.MaxNeighbors < 2 ||
		limits.MaxNeighbors > MaxSupportedNeighbors || limits.MaxEfConstruction < 2 ||
		limits.MaxEfConstruction > MaxEfConstruction {
		return ErrGraphLimitExceeded
	}
	return nil
}

func validVectorFileReference(reference VectorFileReference) bool {
	return reference.Size != 0 && reference.SHA256 != [sha256.Size]byte{}
}
