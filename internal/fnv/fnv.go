package fnv

func Sum32a[T ~string | ~[]byte](data T) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(data); i++ {
		h ^= uint32(data[i])
		h *= 16777619
	}
	return h
}
