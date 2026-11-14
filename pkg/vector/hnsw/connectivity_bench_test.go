package hnsw

import (
	"context"
	"testing"
)

const connectivityBenchmarkNodes = 250_000

func BenchmarkValidateLevel0Reachability(b *testing.B) {
	builder := newConnectivityBenchmarkBuilder(b, connectivityBenchmarkNodes, false)
	b.ReportAllocs()
	for b.Loop() {
		state, err := validateLevel0Reachability(context.Background(), builder.graph)
		if err != nil {
			b.Fatal(err)
		}
		if state.reachableCount != connectivityBenchmarkNodes {
			b.Fatalf("reachable nodes = %d, want %d", state.reachableCount, connectivityBenchmarkNodes)
		}
	}
}

func BenchmarkRepairLevel0Connectivity(b *testing.B) {
	for range b.N {
		b.StopTimer()
		builder := newConnectivityBenchmarkBuilder(b, connectivityBenchmarkNodes, true)
		state, err := validateLevel0Reachability(context.Background(), builder.graph)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		stats, err := builder.repairLevel0Connectivity(context.Background(), state)
		if err != nil {
			b.Fatal(err)
		}
		if stats.addedLinks != 1 {
			b.Fatalf("repair links = %d, want 1", stats.addedLinks)
		}
	}
}

func newConnectivityBenchmarkBuilder(tb testing.TB, nodes int, disconnectLast bool) *builder {
	tb.Helper()
	values := make([]float32, nodes)
	links := make([][]nodeOrdinal, nodes)
	connected := nodes
	if disconnectLast {
		connected--
	}
	for node := range nodes {
		values[node] = float32(node)
		if node > 0 && node < connected {
			links[node] = append(links[node], nodeOrdinal(node-1))
		}
		if node+1 < connected {
			links[node] = append(links[node], nodeOrdinal(node+1))
		}
	}
	return newConnectivityTestBuilder(tb, values, links, 2)
}
