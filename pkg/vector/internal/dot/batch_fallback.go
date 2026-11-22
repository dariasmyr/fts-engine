//go:build !arm64

package dot

func Batch2(target, v0, v1 []float32) (float64, float64) {
	return Float64(target, v0), Float64(target, v1)
}

func Batch4(target, v0, v1, v2, v3 []float32) [4]float64 {
	return [4]float64{
		Float64(target, v0),
		Float64(target, v1),
		Float64(target, v2),
		Float64(target, v3),
	}
}
