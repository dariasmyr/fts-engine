//go:build arm64 && (!go1.27 || !goexperiment.simd)

package vector

// distanceBatch4 calculates the dot product with four vectors in a single pass
func distanceBatch4(target, v0, v1, v2, v3 []float32) [4]float64 {
	// we still calculate each vector in 8 elements at a time
	//
	// but now we have 4 vectors to calculate,
	// and we can calculate thwm independently, so we need
	// to keep track of 8 partial sums for each vector
	// accumulator structure:
	// s[vector_index][accumulator_index]
	var s00, s01, s02, s03, s04, s05, s06, s07 float64 // accumulators for vector 1
	var s10, s11, s12, s13, s14, s15, s16, s17 float64 // accumulators for vector 2
	var s20, s21, s22, s23, s24, s25, s26, s27 float64 // accumulators for vector 3
	var s30, s31, s32, s33, s34, s35, s36, s37 float64 // accumulators for vector 4

	i := 0
	// start calculating 8 elements of a vector at a time,
	// and for each iteration, we calculate the dot product for all 4 vectors
	for ; i+7 < len(target); i += 8 {
		// load each target component once, into `a` variable, and reuse it across all 4 dot products
		a0, a1 := float64(target[i]), float64(target[i+1])
		a2, a3 := float64(target[i+2]), float64(target[i+3])
		a4, a5 := float64(target[i+4]), float64(target[i+5])
		a6, a7 := float64(target[i+6]), float64(target[i+7])

		// accumulate for vector 0
		s00 += a0 * float64(v0[i])
		s01 += a1 * float64(v0[i+1])
		s02 += a2 * float64(v0[i+2])
		s03 += a3 * float64(v0[i+3])
		s04 += a4 * float64(v0[i+4])
		s05 += a5 * float64(v0[i+5])
		s06 += a6 * float64(v0[i+6])
		s07 += a7 * float64(v0[i+7])
		// accumulate for vector 1
		s10 += a0 * float64(v1[i])
		s11 += a1 * float64(v1[i+1])
		s12 += a2 * float64(v1[i+2])
		s13 += a3 * float64(v1[i+3])
		s14 += a4 * float64(v1[i+4])
		s15 += a5 * float64(v1[i+5])
		s16 += a6 * float64(v1[i+6])
		s17 += a7 * float64(v1[i+7])
		// accumulate for vector 2
		s20 += a0 * float64(v2[i])
		s21 += a1 * float64(v2[i+1])
		s22 += a2 * float64(v2[i+2])
		s23 += a3 * float64(v2[i+3])
		s24 += a4 * float64(v2[i+4])
		s25 += a5 * float64(v2[i+5])
		s26 += a6 * float64(v2[i+6])
		s27 += a7 * float64(v2[i+7])
		// accumulate for vector 3
		s30 += a0 * float64(v3[i])
		s31 += a1 * float64(v3[i+1])
		s32 += a2 * float64(v3[i+2])
		s33 += a3 * float64(v3[i+3])
		s34 += a4 * float64(v3[i+4])
		s35 += a5 * float64(v3[i+5])
		s36 += a6 * float64(v3[i+6])
		s37 += a7 * float64(v3[i+7])
	}
	// now we have 8 sums for each vector, we need to combine them into a single result for each vector
	result := [4]float64{
		// use ILP again: instead of summing the 8 sums for each vector in a single line, we can sum them in pairs,
		// and then sum the pairs (a balanced reduction) to shorten the dependency chain from seven
		// additions to three.
		((s00 + s01) + (s02 + s03)) + ((s04 + s05) + (s06 + s07)),
		((s10 + s11) + (s12 + s13)) + ((s14 + s15) + (s16 + s17)),
		((s20 + s21) + (s22 + s23)) + ((s24 + s25) + (s26 + s27)),
		((s30 + s31) + (s32 + s33)) + ((s34 + s35) + (s36 + s37)),
	}
	// handle the remaining elements that are < 8 elements, we can just do a simple loop for the remaining elements
	for ; i < len(target); i++ {
		a := float64(target[i])
		result[0] += a * float64(v0[i])
		result[1] += a * float64(v1[i])
		result[2] += a * float64(v2[i])
		result[3] += a * float64(v3[i])
	}
	return result
}

// distanceBatch2 calculates the dot product with two vectors in a single pass
// same as distanceBatch4, but for 2 vectors instead of 4
func distanceBatch2(target, v0, v1 []float32) (float64, float64) {
	var s00, s01, s02, s03, s04, s05, s06, s07 float64
	var s10, s11, s12, s13, s14, s15, s16, s17 float64
	i := 0
	for ; i+7 < len(target); i += 8 {
		a0, a1 := float64(target[i]), float64(target[i+1])
		a2, a3 := float64(target[i+2]), float64(target[i+3])
		a4, a5 := float64(target[i+4]), float64(target[i+5])
		a6, a7 := float64(target[i+6]), float64(target[i+7])
		s00 += a0 * float64(v0[i])
		s01 += a1 * float64(v0[i+1])
		s02 += a2 * float64(v0[i+2])
		s03 += a3 * float64(v0[i+3])
		s04 += a4 * float64(v0[i+4])
		s05 += a5 * float64(v0[i+5])
		s06 += a6 * float64(v0[i+6])
		s07 += a7 * float64(v0[i+7])
		s10 += a0 * float64(v1[i])
		s11 += a1 * float64(v1[i+1])
		s12 += a2 * float64(v1[i+2])
		s13 += a3 * float64(v1[i+3])
		s14 += a4 * float64(v1[i+4])
		s15 += a5 * float64(v1[i+5])
		s16 += a6 * float64(v1[i+6])
		s17 += a7 * float64(v1[i+7])
	}
	r0 := ((s00 + s01) + (s02 + s03)) + ((s04 + s05) + (s06 + s07))
	r1 := ((s10 + s11) + (s12 + s13)) + ((s14 + s15) + (s16 + s17))
	for ; i < len(target); i++ {
		a := float64(target[i])
		r0 += a * float64(v0[i])
		r1 += a * float64(v1[i])
	}
	return r0, r1
}
