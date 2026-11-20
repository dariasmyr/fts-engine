package format

import "math"

// CheckedMultiplyUint64 multiplies a and b and reports whether the result fits.
func CheckedMultiplyUint64(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}

// CheckedAddUint64 adds a and b and reports whether the result fits.
func CheckedAddUint64(a, b uint64) (uint64, bool) {
	if b > math.MaxUint64-a {
		return 0, false
	}
	return a + b, true
}
