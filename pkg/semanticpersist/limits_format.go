package semanticpersist

import "math"

func normalizeLimits(limits Limits) Limits {
	defaults := DefaultLimits()
	if limits.MaxFileBytes == 0 {
		limits.MaxFileBytes = defaults.MaxFileBytes
	}
	if limits.MaxVectorBytes == 0 {
		limits.MaxVectorBytes = defaults.MaxVectorBytes
	}
	if limits.MaxGraphBytes == 0 {
		limits.MaxGraphBytes = defaults.MaxGraphBytes
	}
	if limits.MaxGraphLinks == 0 {
		limits.MaxGraphLinks = defaults.MaxGraphLinks
	}
	if limits.MaxOpenBytes == 0 {
		limits.MaxOpenBytes = defaults.MaxOpenBytes
	}
	if limits.MaxEfSearch == 0 {
		limits.MaxEfSearch = defaults.MaxEfSearch
	}
	if limits.MaxVisitLimit == 0 {
		limits.MaxVisitLimit = defaults.MaxVisitLimit
	}
	if limits.MaxDimensions == 0 {
		limits.MaxDimensions = defaults.MaxDimensions
	}
	if limits.MaxVectors == 0 {
		limits.MaxVectors = defaults.MaxVectors
	}
	if limits.MaxDocuments == 0 {
		limits.MaxDocuments = defaults.MaxDocuments
	}
	if limits.MaxStringBytes == 0 {
		limits.MaxStringBytes = defaults.MaxStringBytes
	}
	if limits.MaxChunksPerDocument == 0 {
		limits.MaxChunksPerDocument = defaults.MaxChunksPerDocument
	}
	if limits.MaxK == 0 {
		limits.MaxK = defaults.MaxK
	}
	return limits
}

func validateLimits(limits Limits) error {
	if limits.MaxFileBytes == 0 || limits.MaxFileBytes >= math.MaxInt64 || limits.MaxVectorBytes == 0 || limits.MaxVectorBytes > math.MaxUint64-128 ||
		limits.MaxGraphBytes == 0 || limits.MaxGraphBytes >= math.MaxInt64 || limits.MaxGraphLinks == 0 ||
		limits.MaxOpenBytes < limits.MaxFileBytes || limits.MaxOpenBytes >= math.MaxInt64 || limits.MaxEfSearch <= 0 || limits.MaxVisitLimit <= 0 ||
		limits.MaxDimensions <= 0 || limits.MaxVectors <= 0 || limits.MaxDocuments <= 0 || limits.MaxStringBytes <= 0 ||
		limits.MaxChunksPerDocument <= 0 || limits.MaxK <= 0 || uint64(limits.MaxVectors) >= math.MaxUint32 ||
		uint64(limits.MaxDocuments) > math.MaxUint32 || uint64(limits.MaxStringBytes) > math.MaxUint32 ||
		uint64(limits.MaxChunksPerDocument) > math.MaxUint32 || uint64(limits.MaxK) > math.MaxUint32 {
		return ErrLimitExceeded
	}
	return nil
}

func checkedAdd64(a, b uint64) (uint64, bool) {
	if b > math.MaxUint64-a {
		return 0, false
	}
	return a + b, true
}

func checkedMultiply64(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}
