//go:build arm64 && (!go1.27 || !goexperiment.simd)

package dot

func Float64(a, b []float32) float64 {
	// Callers validate dimensions before reaching this hot path.
	var sum0, sum1, sum2, sum3 float64
	var sum4, sum5, sum6, sum7 float64
	i := 0
	for ; i+7 < len(a); i += 8 {
		sum0 += float64(a[i]) * float64(b[i])
		sum1 += float64(a[i+1]) * float64(b[i+1])
		sum2 += float64(a[i+2]) * float64(b[i+2])
		sum3 += float64(a[i+3]) * float64(b[i+3])
		sum4 += float64(a[i+4]) * float64(b[i+4])
		sum5 += float64(a[i+5]) * float64(b[i+5])
		sum6 += float64(a[i+6]) * float64(b[i+6])
		sum7 += float64(a[i+7]) * float64(b[i+7])
	}
	sum01 := sum0 + sum1
	sum23 := sum2 + sum3
	sum45 := sum4 + sum5
	sum67 := sum6 + sum7
	sum := (sum01 + sum23) + (sum45 + sum67)
	for ; i < len(a); i++ {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}
