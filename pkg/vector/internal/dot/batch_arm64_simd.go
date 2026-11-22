//go:build arm64 && go1.27 && goexperiment.simd

package dot

import "simd/archsimd"

// distanceBatch4 calculates the dot product of target vector with four vectors in a single pass
func Batch4(target, v0, v1, v2, v3 []float32) [4]float64 {
	// accumulators: for each vector we create a pair of SIMD registers (2*float64 values)
	// that will be accumulate values and summed at the end to get the final result
	var low0, high0, low1, high1 archsimd.Float64x2 // vector 0 and vector 1 accumulators
	var low2, high2, low3, high3 archsimd.Float64x2 // vector 2 and vector 3 accumulators

	i := 0
	for ; i+3 < len(target); i += 4 {
		// create a SIMD register with 4 float32 values from the target slice
		t := archsimd.LoadFloat32x4(target[i : i+4])

		// widen (convert to float64) first 2 float32 element of target register
		tLow := t.ConvertLo2ToFloat64()
		// widen (convert to float64) last 2 float32 element of target register
		tHigh := t.HiToLo().ConvertLo2ToFloat64()

		// create a SIMD register with 4 float32 values from each vector slice
		// block semantic: block[vector_index] = vector[vector_index][i:i+4]
		b0 := archsimd.LoadFloat32x4(v0[i : i+4])
		b1 := archsimd.LoadFloat32x4(v1[i : i+4])
		b2 := archsimd.LoadFloat32x4(v2[i : i+4])
		b3 := archsimd.LoadFloat32x4(v3[i : i+4])

		// widen (convert to float64) the first 2 float32 elements of each vector register
		// then accumulate (multiply + add) them with the first 2 float64 elements of the target register

		// accumulate for vector 0
		low0 = tLow.MulAdd(b0.ConvertLo2ToFloat64(), low0)
		high0 = tHigh.MulAdd(b0.HiToLo().ConvertLo2ToFloat64(), high0)
		// accumulate for vector 1
		low1 = tLow.MulAdd(b1.ConvertLo2ToFloat64(), low1)
		high1 = tHigh.MulAdd(b1.HiToLo().ConvertLo2ToFloat64(), high1)
		// accumulate for vector 2
		low2 = tLow.MulAdd(b2.ConvertLo2ToFloat64(), low2)
		high2 = tHigh.MulAdd(b2.HiToLo().ConvertLo2ToFloat64(), high2)
		// accumulate for vector 3
		low3 = tLow.MulAdd(b3.ConvertLo2ToFloat64(), low3)
		high3 = tHigh.MulAdd(b3.HiToLo().ConvertLo2ToFloat64(), high3)
	}

	result := [4]float64{
		sumSIMDAccumulators(low0, high0),
		sumSIMDAccumulators(low1, high1),
		sumSIMDAccumulators(low2, high2),
		sumSIMDAccumulators(low3, high3),
	}

	// handle the remaining elements that are < 4 elements, we can just do a simple loop for the remaining elements
	for ; i < len(target); i++ {
		t := float64(target[i])
		result[0] += t * float64(v0[i])
		result[1] += t * float64(v1[i])
		result[2] += t * float64(v2[i])
		result[3] += t * float64(v3[i])
	}
	return result
}

func sumSIMDAccumulators(low, high archsimd.Float64x2) float64 {
	var lowSums, highSums [2]float64
	// store accumulator values to to arrays
	low.StoreArray(&lowSums)
	high.StoreArray(&highSums)

	// sum the 4 float64 values to get the final result
	return (lowSums[0] + lowSums[1]) + (highSums[0] + highSums[1])
}

// distanceBatch2 calculates the dot product with two vectors in a single pass
// same as distanceBatch4, but for 2 vectors instead of 4
func Batch2(target, v0, v1 []float32) (float64, float64) {
	var low0, high0, low1, high1 archsimd.Float64x2
	i := 0
	for ; i+3 < len(target); i += 4 {
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
	r0 := sumSIMDAccumulators(low0, high0)
	r1 := sumSIMDAccumulators(low1, high1)
	for ; i < len(target); i++ {
		t := float64(target[i])
		r0 += t * float64(v0[i])
		r1 += t * float64(v1[i])
	}
	return r0, r1
}
