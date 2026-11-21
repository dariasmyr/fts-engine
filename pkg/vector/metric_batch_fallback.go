//go:build !arm64

package vector

func distanceBatch2(target, v0, v1 []float32) (float64, float64) {
	return dotFloat64(target, v0), dotFloat64(target, v1)
}

func distanceBatch4(target, v0, v1, v2, v3 []float32) [4]float64 {
	return [4]float64{
		dotFloat64(target, v0),
		dotFloat64(target, v1),
		dotFloat64(target, v2),
		dotFloat64(target, v3),
	}
}
