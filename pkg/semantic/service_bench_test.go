package semantic

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

var (
	benchmarkPublishedIDs   []uint64
	benchmarkPendingVectors int
	benchmarkDiscardErr     error
	benchmarkSliceVersions  map[fts.DocID][]uint64
	benchmarkRangeVersions  map[fts.DocID]documentVersion
)

func BenchmarkCurrentByDocRepresentation(b *testing.B) {
	tests := []struct {
		name      string
		documents int
		chunks    int
	}{
		{name: "documents=1000000/chunks=1", documents: 1_000_000, chunks: 1},
		{name: "documents=100000/chunks=10", documents: 100_000, chunks: 10},
	}

	for _, test := range tests {
		docIDs := make([]fts.DocID, test.documents)
		for i := range docIDs {
			docIDs[i] = fts.DocID(strconv.Itoa(i))
		}

		b.Run("slice/"+test.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				versions := make(map[fts.DocID][]uint64, test.documents)
				for i, docID := range docIDs {
					first := uint64(i*test.chunks + 1)
					ids := make([]uint64, test.chunks)
					for j := range ids {
						ids[j] = first + uint64(j)
					}
					versions[docID] = ids
				}
				benchmarkSliceVersions = versions
			}
		})

		b.Run("range/"+test.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				versions := make(map[fts.DocID]documentVersion, test.documents)
				for i, docID := range docIDs {
					versions[docID] = documentVersion{
						firstVectorID: uint64(i*test.chunks + 1),
						vectorCount:   test.chunks,
					}
				}
				benchmarkRangeVersions = versions
			}
		})
	}
}

func BenchmarkDiscardSupersededVersion(b *testing.B) {
	const documentVectorCount = 4

	for _, pendingCount := range []int{1_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("published/P=%d", pendingCount), func(b *testing.B) {
			service := benchmarkServiceWithPendingVectors(pendingCount)
			version := documentVersion{firstVectorID: 1, vectorCount: documentVectorCount}
			service.locations = make(map[uint64]vectorLocation, version.vectorCount)
			for i := range version.vectorCount {
				id := version.vectorID(i)
				service.locations[id] = vectorLocation{}
			}
			b.ReportAllocs()
			b.ReportMetric(float64(pendingCount), "pending_vectors")
			b.ResetTimer()
			for b.Loop() {
				benchmarkPublishedIDs, benchmarkDiscardErr = service.discardSupersededVersion(version)
			}
			if benchmarkDiscardErr != nil {
				b.Fatal(benchmarkDiscardErr)
			}
		})

		for _, position := range []string{"head", "tail"} {
			b.Run(fmt.Sprintf("pending/%s/P=%d", position, pendingCount), func(b *testing.B) {
				service := benchmarkServiceWithPendingVectors(pendingCount)
				firstID := uint64(1)
				if position == "tail" {
					firstID = uint64(pendingCount - documentVectorCount + 1)
				}
				version := documentVersion{firstVectorID: firstID, vectorCount: documentVectorCount}
				b.ReportAllocs()
				b.ReportMetric(float64(pendingCount), "pending_vectors")
				b.ResetTimer()
				for b.Loop() {
					b.StopTimer()
					resetPendingVectors(service, pendingCount, position, documentVectorCount)
					b.StartTimer()
					benchmarkPublishedIDs, benchmarkDiscardErr = service.discardSupersededVersion(version)
					benchmarkPendingVectors = len(service.pendingVectors)
				}
				if benchmarkDiscardErr != nil {
					b.Fatal(benchmarkDiscardErr)
				}
			})
		}
	}
}

func benchmarkServiceWithPendingVectors(count int) *Service {
	pending := make([]pendingVector, count)
	for i := range pending {
		pending[i].row.VectorID = uint64(i + 1)
	}
	return &Service{
		pendingVectors: pending,
		locations:      make(map[uint64]vectorLocation),
	}
}

func resetPendingVectors(service *Service, count int, position string, removed int) {
	if len(service.pendingVectors) == count {
		return
	}
	service.pendingVectors = service.pendingVectors[:count]
	if position == "head" {
		copy(service.pendingVectors[removed:], service.pendingVectors[:count-removed])
		for i := range removed {
			service.pendingVectors[i].row.VectorID = uint64(i + 1)
		}
		return
	}
	for i := count - removed; i < count; i++ {
		service.pendingVectors[i].row.VectorID = uint64(i + 1)
	}
}
