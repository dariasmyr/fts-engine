//go:build arm64 && go1.27 && goexperiment.simd

package vector

import "simd/archsimd"

func dotFloat64(a, b []float32) float64 {
	var low, high archsimd.Float64x2
	i := 0
	for ; i+3 < len(a); i += 4 {
		av := archsimd.LoadFloat32x4(a[i : i+4])
		bv := archsimd.LoadFloat32x4(b[i : i+4])
		low = av.ConvertLo2ToFloat64().MulAdd(bv.ConvertLo2ToFloat64(), low)
		high = av.HiToLo().ConvertLo2ToFloat64().MulAdd(
			bv.HiToLo().ConvertLo2ToFloat64(),
			high,
		)
	}
	var lowSums, highSums [2]float64
	low.StoreArray(&lowSums)
	high.StoreArray(&highSums)
	sum := (lowSums[0] + lowSums[1]) + (highSums[0] + highSums[1])
	for ; i < len(a); i++ {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}
