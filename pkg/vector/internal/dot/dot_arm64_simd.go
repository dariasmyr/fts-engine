//go:build arm64 && go1.27 && goexperiment.simd

package dot

import "simd/archsimd"

// dotFloat64 calculates the dot product of two vectors and returns the result (dot product)
func Float64(a, b []float32) float64 {
	// accumulators: pair of SIMD registers (2*float64 values)
	// we will sum them at the end to get the final result
	var low, high archsimd.Float64x2

	i := 0

	// start calculating 4 elements of two vectors at a time
	for ; i+3 < len(a); i += 4 {
		// create a SIMD register and load 4 float32 values from `a` and `b` slices
		av := archsimd.LoadFloat32x4(a[i : i+4])
		bv := archsimd.LoadFloat32x4(b[i : i+4])

		// widen (convert to float64) the first 2 float32 elements of `av` register
		// then accumulate (multiply + add) them with the first 2 float64 elements of the `bv` accumulator
		low = av.ConvertLo2ToFloat64().MulAdd(bv.ConvertLo2ToFloat64(), low)

		// moves the last 2 float32 elements of `av` register to the first 2 elements of the `av` register
		// then converts them to float64 and accumulate (multiply + add) them with the last 2 float64 elements of the `bv` accumulator
		high = av.HiToLo().ConvertLo2ToFloat64().MulAdd(
			bv.HiToLo().ConvertLo2ToFloat64(),
			high,
		)
	}

	// store accumulator values to to arrays
	var lowSums, highSums [2]float64

	low.StoreArray(&lowSums)
	high.StoreArray(&highSums)

	// sum the accumulator values and return the final result
	sum := (lowSums[0] + lowSums[1]) + (highSums[0] + highSums[1])

	// handle the remaining elements that are < 4 elements, we can just do a simple loop for the remaining elements
	for ; i < len(a); i++ {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}
