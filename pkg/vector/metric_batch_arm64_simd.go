//go:build arm64 && go1.27 && goexperiment.simd

package vector

import "simd/archsimd"

func distanceBatch2(target, v0, v1 []float32) (float64, float64) {
	var low0, high0, low1, high1 archsimd.Float64x2
	i := 0
	for ; i+3 < len(target); i += 4 {
		// Load and widen target lanes once, then reuse them for both vectors.
		t := archsimd.LoadFloat32x4(target[i : i+4])
		tLow := t.ConvertLo2ToFloat64()
		tHigh := t.HiToLo().ConvertLo2ToFloat64()
		b0 := archsimd.LoadFloat32x4(v0[i : i+4])
		b1 := archsimd.LoadFloat32x4(v1[i : i+4])
		low0 = tLow.MulAdd(b0.ConvertLo2ToFloat64(), low0)
		high0 = tHigh.MulAdd(b0.HiToLo().ConvertLo2ToFloat64(), high0)
		low1 = tLow.MulAdd(b1.ConvertLo2ToFloat64(), low1)
		high1 = tHigh.MulAdd(b1.HiToLo().ConvertLo2ToFloat64(), high1)
	}
	r0 := reduceBatchSIMD(low0, high0)
	r1 := reduceBatchSIMD(low1, high1)
	for ; i < len(target); i++ {
		t := float64(target[i])
		r0 += t * float64(v0[i])
		r1 += t * float64(v1[i])
	}
	return r0, r1
}

func distanceBatch4(target, v0, v1, v2, v3 []float32) [4]float64 {
	var low0, high0, low1, high1 archsimd.Float64x2
	var low2, high2, low3, high3 archsimd.Float64x2
	i := 0
	for ; i+3 < len(target); i += 4 {
		// Load and widen target lanes once, then reuse them for all four vectors.
		t := archsimd.LoadFloat32x4(target[i : i+4])
		tLow := t.ConvertLo2ToFloat64()
		tHigh := t.HiToLo().ConvertLo2ToFloat64()
		b0 := archsimd.LoadFloat32x4(v0[i : i+4])
		b1 := archsimd.LoadFloat32x4(v1[i : i+4])
		b2 := archsimd.LoadFloat32x4(v2[i : i+4])
		b3 := archsimd.LoadFloat32x4(v3[i : i+4])
		low0 = tLow.MulAdd(b0.ConvertLo2ToFloat64(), low0)
		high0 = tHigh.MulAdd(b0.HiToLo().ConvertLo2ToFloat64(), high0)
		low1 = tLow.MulAdd(b1.ConvertLo2ToFloat64(), low1)
		high1 = tHigh.MulAdd(b1.HiToLo().ConvertLo2ToFloat64(), high1)
		low2 = tLow.MulAdd(b2.ConvertLo2ToFloat64(), low2)
		high2 = tHigh.MulAdd(b2.HiToLo().ConvertLo2ToFloat64(), high2)
		low3 = tLow.MulAdd(b3.ConvertLo2ToFloat64(), low3)
		high3 = tHigh.MulAdd(b3.HiToLo().ConvertLo2ToFloat64(), high3)
	}
	result := [4]float64{
		reduceBatchSIMD(low0, high0),
		reduceBatchSIMD(low1, high1),
		reduceBatchSIMD(low2, high2),
		reduceBatchSIMD(low3, high3),
	}
	for ; i < len(target); i++ {
		t := float64(target[i])
		result[0] += t * float64(v0[i])
		result[1] += t * float64(v1[i])
		result[2] += t * float64(v2[i])
		result[3] += t * float64(v3[i])
	}
	return result
}

func reduceBatchSIMD(low, high archsimd.Float64x2) float64 {
	var lowSums, highSums [2]float64
	low.StoreArray(&lowSums)
	high.StoreArray(&highSums)
	return (lowSums[0] + lowSums[1]) + (highSums[0] + highSums[1])
}
