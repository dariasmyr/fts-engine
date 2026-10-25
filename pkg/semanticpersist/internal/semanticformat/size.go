package semanticformat

import "math"

func addEncodedSize(total *uint64, size uint64) bool {
	if size > math.MaxUint64-*total {
		return false
	}
	*total += size
	return true
}

func addEncodedStringSize(total *uint64, value string) bool {
	return addEncodedSize(total, wireStringLengthPrefixSize+uint64(len(value)))
}

func checkedMultiply(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}

func checkedAdd(a, b uint64) (uint64, bool) {
	if b > math.MaxUint64-a {
		return 0, false
	}
	return a + b, true
}
