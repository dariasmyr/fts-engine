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

	after.HeapAlloc -= before.HeapAlloc
	after.TotalAlloc -= before.TotalAlloc
	after.HeapObjects -= before.HeapObjects

	return after
}
