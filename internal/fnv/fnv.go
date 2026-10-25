// Package fnv provides allocation-free FNV hashing helpers.
package fnv

// Sum32a returns the 32-bit FNV-1a hash of data.
func Sum32a[T ~string | ~[]byte](data T) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(data); i++ {
		h ^= uint32(data[i])
		h *= 16777619
	}
	return h
}
