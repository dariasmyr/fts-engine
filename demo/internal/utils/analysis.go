package utils

import "runtime"

func MeasureMemory(build func()) runtime.MemStats {
	runtime.GC()
	runtime.GC()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	build()

	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)

	after.HeapAlloc = nonNegativeDelta(after.HeapAlloc, before.HeapAlloc)
	after.TotalAlloc = nonNegativeDelta(after.TotalAlloc, before.TotalAlloc)
	after.HeapObjects = nonNegativeDelta(after.HeapObjects, before.HeapObjects)

	return after
}

func nonNegativeDelta(after, before uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}
