package generationformat

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
