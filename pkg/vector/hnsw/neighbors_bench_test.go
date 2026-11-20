package hnsw

import (
	"context"
	"fmt"
	"math"
	"testing"
	"unsafe"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

var (
	neighborSelectionBenchmarkResult neighborSelection
	verificationBenchmarkBuilder     *builder
)

func BenchmarkSelectNeighbors(b *testing.B) {
	builder := newConnectivityTestBuilder(
		b,
		[]float32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17},
		make([][]nodeOrdinal, 18),
		8,
	)
	candidates := make([]candidate, 0, 17)
	for node := nodeOrdinal(1); node <= 17; node++ {
		candidates = append(candidates, candidate{
			node:     node,
			distance: builder.distanceNodes(0, node),
		})
	}

	for _, fillFromRejected := range []bool{true, false} {
		b.Run(fmt.Sprintf("fallback=%t", fillFromRejected), func(b *testing.B) {
			selected := make([]buildNeighbor, 0, 16)
			b.ReportAllocs()
			for b.Loop() {
				selection, err := builder.selectNeighbors(
					context.Background(),
					candidates,
					16,
					fillFromRejected,
					selected,
				)
				if err != nil {
					b.Fatal(err)
				}
				selected = selection.neighbors
			}
		})
	}
}

func BenchmarkIncrementalReversePruning(b *testing.B) {
	values := make([][]float32, 18)
	for node := 1; node <= 16; node++ {
		values[node] = make([]float32, 17)
		values[node][node-1] = 1
	}
	values[0] = make([]float32, 17)
	values[17] = make([]float32, 17)
	for dimension := range values[17] {
		values[17][dimension] = -0.1
	}
	builder := newNeighborSelectionTestBuilder(b, values, 16)

	for _, incremental := range []bool{false, true} {
		b.Run(fmt.Sprintf("incremental=%t", incremental), func(b *testing.B) {
			candidates := make([]candidate, 0, 17)
			for node := nodeOrdinal(1); node <= 17; node++ {
				candidates = append(candidates, candidate{
					node:     node,
					distance: builder.distanceNodes(0, node),
					verified: incremental && node <= 16,
				})
			}
			b.ReportAllocs()
			var selection neighborSelection
			selected := make([]buildNeighbor, 0, 16)
			for b.Loop() {
				var err error
				selection, err = builder.selectNeighbors(context.Background(), candidates, 16, false, selected)
				if err != nil {
					b.Fatal(err)
				}
				selected = selection.neighbors
				neighborSelectionBenchmarkResult = selection
			}
			b.ReportMetric(float64(selection.distanceChecks), "pair-checks/op")
			b.ReportMetric(float64(selection.skippedChecks), "skipped-checks/op")
		})
	}
}

func BenchmarkBuildVerificationMetadata(b *testing.B) {
	const (
		rows       = 1_000
		dimensions = 384
	)
	values := make([]float32, rows*dimensions)
	for row := range rows {
		for dimension := range dimensions {
			values[row*dimensions+dimension] = float32(math.Sin(float64((row + 1) * (dimension + 1))))
		}
	}
	spec, err := vector.NewSpec(dimensions, vector.MetricL2Squared)
	if err != nil {
		b.Fatal(err)
	}
	config := BuildConfig{MaxNeighbors: 16, EfConstruction: 64, Seed: 1}
	matrix, err := newCanonicalTestMatrix(dimensions, vector.MetricL2Squared, values)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	var built *builder
	for b.Loop() {
		built = newBuilder(config, spec, matrix, rows)
		for range rows {
			if err := built.add(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
		verificationBenchmarkBuilder = built
	}
	b.ReportMetric(float64(built.workspace.reverseDistanceChecks), "pair-checks/op")
	b.ReportMetric(float64(built.workspace.reverseSkippedChecks), "skipped-checks/op")
	b.ReportMetric(float64(verifiedLinkMetadataBytes(built))/rows, "metadata-B/node")
	b.ReportMetric(float64(unsafe.Sizeof(buildNeighbor{})-unsafe.Sizeof(nodeOrdinal(0))), "distance-cache-B/link")
}

func verifiedLinkMetadataBytes(b *builder) uintptr {
	type mutableNodeWithoutVerification struct {
		level uint8
		links [][]buildNeighbor
	}
	bytes := uintptr(len(b.graph.nodes)) *
		(unsafe.Sizeof(mutableNode{}) - unsafe.Sizeof(mutableNodeWithoutVerification{}))
	for i := range b.graph.nodes {
		bytes += uintptr(cap(b.graph.nodes[i].verifiedLinkBits)) * unsafe.Sizeof(uint64(0))
	}
	return bytes
}
