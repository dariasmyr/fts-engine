package semantic

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

var (
	phase3CommitState commitState
	phase3Documents   []DocumentHit
	phase3Distinct    int
	phase3LegacyMap   map[VectorID]vectorLocation
	phase3Table       *locationTable
)

func BenchmarkCaptureCommitState(b *testing.B) {
	for _, live := range []int{1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("live=%d", live), func(b *testing.B) {
			index := newBenchmarkIndex(b, live, live, 16)
			index.state.locations = benchmarkLocations(live)
			b.ReportAllocs()
			for b.Loop() {
				state, err := index.captureCommitState(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				phase3CommitState = state
			}
			b.ReportMetric(float64(live), "live-vectors")
		})
	}
}

func BenchmarkLocationTableDelta(b *testing.B) {
	for _, live := range []int{1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("live=%d/delta=1", live), func(b *testing.B) {
			locations := benchmarkLocations(live)
			removed := []VectorID{VectorID(live / 2)}
			added := map[VectorID]vectorLocation{
				VectorID(live + 1): {segment: 2, ordinal: 0},
			}
			b.ReportAllocs()
			for b.Loop() {
				next, err := locations.withChanges(context.Background(), removed, added)
				if err != nil {
					b.Fatal(err)
				}
				if next.count != live {
					b.Fatalf("location count = %d, want %d", next.count, live)
				}
			}
			b.ReportMetric(float64(live), "live-vectors")
		})
	}
}

// BenchmarkLocationUpdateStrategies compares the removed full-map ownership
// strategy with the paged COW strategy on the same logical location set and
// one-remove/one-add delta.
func BenchmarkLocationUpdateStrategies(b *testing.B) {
	for _, live := range []int{100_000, 1_000_000} {
		removed := VectorID(live / 2)
		addedID := VectorID(live + 1)
		addedLocation := vectorLocation{segment: 2, ordinal: 0}

		b.Run(fmt.Sprintf("live=%d/full-map-clone", live), func(b *testing.B) {
			locations := benchmarkLocationMap(live)
			b.ReportAllocs()
			for b.Loop() {
				next := make(map[VectorID]vectorLocation, len(locations))
				for id, location := range locations {
					next[id] = location
				}
				delete(next, removed)
				next[addedID] = addedLocation
				phase3LegacyMap = next
			}
			b.StopTimer()
			phase3LegacyMap = nil
			b.ReportMetric(float64(live), "live-vectors")
		})

		b.Run(fmt.Sprintf("live=%d/paged-cow", live), func(b *testing.B) {
			locations := benchmarkLocations(live)
			removedIDs := []VectorID{removed}
			added := map[VectorID]vectorLocation{addedID: addedLocation}
			b.ReportAllocs()
			for b.Loop() {
				next, err := locations.withChanges(context.Background(), removedIDs, added)
				if err != nil {
					b.Fatal(err)
				}
				phase3Table = next
			}
			b.StopTimer()
			phase3Table = nil
			b.ReportMetric(float64(live), "live-vectors")
		})
	}
}

func BenchmarkBuildPendingSegmentDelta(b *testing.B) {
	config := benchmarkConfig(100, 100, 16)
	space, err := vector.NewSpace(config.Dimensions, config.Metric)
	if err != nil {
		b.Fatal(err)
	}
	for _, delta := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("delta=%d", delta), func(b *testing.B) {
			pending := make([]pendingVector, delta)
			for n := range pending {
				chunk := benchmarkChunk(fts.DocID(fmt.Sprintf("doc-%06d", n)), n)
				prepared, err := space.Prepare(chunk.Embedding)
				if err != nil {
					b.Fatal(err)
				}
				pending[n] = pendingVector{
					row: VectorRow{VectorID: VectorID(n + 1), Chunk: chunk.Ref}, vector: prepared,
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				segment, err := buildPendingSegment(context.Background(), 1, pending, config)
				if err != nil {
					b.Fatal(err)
				}
				if segment == nil {
					b.Fatal("buildPendingSegment() returned nil")
				}
			}
			b.ReportMetric(float64(delta), "delta-vectors")
		})
	}
}

func BenchmarkCommitLifecycleThreshold(b *testing.B) {
	const (
		documents = 128
		maxStale  = 32
	)
	for _, replacements := range []int{maxStale, maxStale + 1} {
		name := fmt.Sprintf("stale=%d/max=%d", replacements, maxStale)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				b.StopTimer()
				index := newBenchmarkIndex(b, documents, maxStale, 16)
				addBenchmarkDocuments(b, index, 0, documents)
				if err := index.Commit(context.Background()); err != nil {
					b.Fatal(err)
				}
				for n := range replacements {
					docID := fts.DocID(fmt.Sprintf("doc-%06d", n))
					if err := index.ReplaceDocument(
						context.Background(),
						docID,
						[]ChunkEmbedding{benchmarkChunk(docID, documents+n)},
					); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				if err := index.Commit(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGroupDocumentHits(b *testing.B) {
	tests := []struct {
		documents  int
		chunks     int
		duplicates int
	}{
		{documents: 100, chunks: 2, duplicates: 1},
		{documents: 100, chunks: 2, duplicates: 4},
		{documents: 1_000, chunks: 2, duplicates: 4},
	}
	for _, test := range tests {
		name := fmt.Sprintf(
			"documents=%d/chunks=%d/duplicates=%d",
			test.documents,
			test.chunks,
			test.duplicates,
		)
		b.Run(name, func(b *testing.B) {
			candidates := benchmarkCandidates(test.documents, test.chunks, test.duplicates)
			b.ReportAllocs()
			for b.Loop() {
				hits, distinct, err := groupDocumentHits(context.Background(), candidates, 10, test.chunks)
				if err != nil {
					b.Fatal(err)
				}
				phase3Documents = hits
				phase3Distinct = distinct
			}
			b.ReportMetric(float64(len(candidates)), "candidates")
		})
	}
}

func BenchmarkIndexSearchStaleRatio(b *testing.B) {
	const physical = 1_000
	for _, stalePercent := range []int{0, 50, 90} {
		b.Run(fmt.Sprintf("physical=%d/stale=%d%%", physical, stalePercent), func(b *testing.B) {
			index := newBenchmarkIndex(b, physical, physical, 2)
			addBenchmarkDocuments(b, index, 0, physical)
			if err := index.Commit(context.Background()); err != nil {
				b.Fatal(err)
			}
			stale := physical * stalePercent / 100
			for n := range stale {
				docID := fts.DocID(fmt.Sprintf("doc-%06d", n))
				if err := index.DeleteDocument(context.Background(), docID); err != nil {
					b.Fatal(err)
				}
			}
			if stale > 0 {
				if err := index.Commit(context.Background()); err != nil {
					b.Fatal(err)
				}
			}

			query := benchmarkChunk("query", physical-1)
			want := exactBenchmarkDocIDs(query.Embedding, stale, physical, 10)
			config := QueryConfig{CandidateChunks: 64, EfSearch: 64, VisitLimit: 128}
			var visited, computations, operations int
			var recall float64
			b.ReportAllocs()
			for b.Loop() {
				result, err := index.Search(context.Background(), []ChunkEmbedding{query}, 10, config)
				if err != nil {
					b.Fatal(err)
				}
				recall = benchmarkRecall(result.Hits, want)
				visited += result.Stats.VisitedNodes
				computations += result.Stats.DistanceComputations
				operations++
			}
			b.ReportMetric(float64(physical-stale), "live-vectors")
			b.ReportMetric(recall, "recall@10")
			b.ReportMetric(float64(visited)/float64(operations), "visited/op")
			b.ReportMetric(float64(computations)/float64(operations), "distances/op")
		})
	}
}

func BenchmarkVisitSharePolicy(b *testing.B) {
	const (
		segmentRows = 500
		staleRows   = 450
		visitBudget = 128
	)
	index := newBenchmarkIndex(b, segmentRows*2, segmentRows*2, 3)
	addBenchmarkDocuments(b, index, 0, segmentRows)
	if err := index.Commit(context.Background()); err != nil {
		b.Fatal(err)
	}
	addBenchmarkDocuments(b, index, segmentRows, segmentRows)
	if err := index.Commit(context.Background()); err != nil {
		b.Fatal(err)
	}
	for n := range staleRows {
		docID := fts.DocID(fmt.Sprintf("doc-%06d", n))
		if err := index.DeleteDocument(context.Background(), docID); err != nil {
			b.Fatal(err)
		}
	}
	if err := index.Commit(context.Background()); err != nil {
		b.Fatal(err)
	}
	view := index.View()
	physical := make([]int, len(view.segments))
	live := make([]int, len(view.segments))
	mixed := make([]int, len(view.segments))
	for n, segment := range view.segments {
		physical[n] = segment.segment.len()
		live[n] = segment.liveness.AllowedOrdinalCount()
		mixed[n] = physical[n] + live[n]
	}
	policies := []struct {
		name       string
		capacities []int
	}{
		{name: "physical", capacities: physical},
		{name: "live", capacities: live},
		{name: "physical+live", capacities: mixed},
	}
	queries := []struct {
		name string
		row  int
	}{
		{name: "stale-heavy-segment", row: segmentRows - 1},
		{name: "fully-live-segment", row: segmentRows*2 - 1},
	}
	for _, query := range queries {
		queryChunk := benchmarkChunk("query", query.row)
		prepared, err := view.space.Prepare(queryChunk.Embedding)
		if err != nil {
			b.Fatal(err)
		}
		want := exactBenchmarkDocIDs(queryChunk.Embedding, staleRows, segmentRows*2, 10)
		for _, policy := range policies {
			b.Run(query.name+"/"+policy.name, func(b *testing.B) {
				var visited, computations, operations int
				var recall float64
				b.ReportAllocs()
				for b.Loop() {
					hits, stats, err := benchmarkSearchWithVisitShares(
						view, prepared, policy.capacities, visitBudget,
					)
					if err != nil {
						b.Fatal(err)
					}
					recall = benchmarkChunkRecall(hits, want)
					visited += stats.VisitedNodes
					computations += stats.DistanceComputations
					operations++
				}
				b.ReportMetric(recall, "recall@10")
				b.ReportMetric(float64(visited)/float64(operations), "visited/op")
				b.ReportMetric(float64(computations)/float64(operations), "distances/op")
			})
		}
	}
}

func BenchmarkReplaceBeforeCommit(b *testing.B) {
	for _, pendingCount := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("pending=%d", pendingCount), func(b *testing.B) {
			index := newBenchmarkIndex(b, pendingCount, pendingCount, 16)
			addBenchmarkDocuments(b, index, 0, pendingCount)
			docIDs := make([]fts.DocID, pendingCount)
			batches := make([][]ChunkEmbedding, pendingCount)
			for n := range pendingCount {
				docIDs[n] = fts.DocID(fmt.Sprintf("doc-%06d", n))
				batches[n] = []ChunkEmbedding{benchmarkChunk(docIDs[n], n)}
			}
			b.ReportAllocs()
			operation := 0
			for b.Loop() {
				target := operation % pendingCount
				if err := index.ReplaceDocument(
					context.Background(),
					docIDs[target],
					batches[target],
				); err != nil {
					b.Fatal(err)
				}
				operation++
			}
			b.ReportMetric(float64(pendingCount), "pending-vectors")
		})
	}
}

func benchmarkLocations(count int) *locationTable {
	locations := newLocationTable()
	for n := range count {
		if err := locations.set(
			VectorID(n+1),
			vectorLocation{segment: 1, ordinal: vector.Ordinal(n)},
		); err != nil {
			panic(err)
		}
	}
	return locations
}

func benchmarkLocationMap(count int) map[VectorID]vectorLocation {
	locations := make(map[VectorID]vectorLocation, count)
	for n := range count {
		locations[VectorID(n+1)] = vectorLocation{segment: 1, ordinal: vector.Ordinal(n)}
	}
	return locations
}

func benchmarkCandidates(documents, chunks, duplicates int) []ChunkHit {
	candidates := make([]ChunkHit, 0, documents*chunks*duplicates)
	for duplicate := range duplicates {
		for document := range documents {
			docID := fts.DocID(fmt.Sprintf("doc-%06d", document))
			for chunk := range chunks {
				candidates = append(candidates, ChunkHit{
					Ref: Ref{
						ID: ChunkID(fmt.Sprintf("chunk-%06d", chunk)), DocID: docID,
						Field: fts.DefaultField, EndByte: 1,
					},
					Distance: float64(document*chunks+chunk) + float64(duplicate)/10,
				})
			}
		}
	}
	return candidates
}

func exactBenchmarkDocIDs(query []float32, firstLive, physical, k int) []fts.DocID {
	type exactHit struct {
		docID    fts.DocID
		distance float64
	}
	hits := make([]exactHit, 0, physical-firstLive)
	for n := firstLive; n < physical; n++ {
		candidate := benchmarkChunk(fts.DocID(fmt.Sprintf("doc-%06d", n)), n)
		var distance float64
		for dimension, value := range candidate.Embedding {
			delta := float64(query[dimension] - value)
			distance += delta * delta
		}
		hits = append(hits, exactHit{docID: candidate.Ref.DocID, distance: distance})
	}
	slices.SortFunc(hits, func(a, b exactHit) int {
		if a.distance < b.distance {
			return -1
		}
		if a.distance > b.distance {
			return 1
		}
		return cmp.Compare(a.docID, b.docID)
	})
	k = min(k, len(hits))
	result := make([]fts.DocID, k)
	for n := range k {
		result[n] = hits[n].docID
	}
	return result
}

func benchmarkRecall(got []DocumentHit, want []fts.DocID) float64 {
	wanted := make(map[fts.DocID]struct{}, len(want))
	for _, docID := range want {
		wanted[docID] = struct{}{}
	}
	matches := 0
	for _, hit := range got {
		if _, ok := wanted[hit.DocID]; ok {
			matches++
		}
	}
	return float64(matches) / float64(len(want))
}

func benchmarkSearchWithVisitShares(
	view *IndexView,
	query vector.PreparedVector,
	capacities []int,
	visitBudget int,
) ([]ChunkHit, hnsw.SearchStats, error) {
	shares, err := allocateVisitShares(context.Background(), 1, capacities, visitBudget)
	if err != nil {
		return nil, hnsw.SearchStats{}, err
	}
	merged := make(map[fts.DocID]ChunkHit)
	stats := hnsw.SearchStats{Termination: hnsw.TerminationComplete}
	for n, segment := range view.segments {
		partial, err := searchSegmentChunksPrepared(
			context.Background(),
			view.space,
			segment,
			query,
			min(64, segment.liveness.AllowedOrdinalCount()),
			hnsw.QueryConfig{EfSearch: 64, VisitLimit: shares[0][n]},
		)
		if err != nil {
			return nil, hnsw.SearchStats{}, err
		}
		mergeSearchStats(&stats, partial.Stats)
		for _, hit := range partial.Hits {
			previous, exists := merged[hit.Ref.DocID]
			if !exists || compareChunkHits(hit, previous) < 0 {
				merged[hit.Ref.DocID] = hit
			}
		}
	}
	hits := make([]ChunkHit, 0, len(merged))
	for _, hit := range merged {
		hits = append(hits, hit)
	}
	slices.SortFunc(hits, compareChunkHits)
	return hits[:min(10, len(hits))], stats, nil
}

func benchmarkChunkRecall(got []ChunkHit, want []fts.DocID) float64 {
	wanted := make(map[fts.DocID]struct{}, len(want))
	for _, docID := range want {
		wanted[docID] = struct{}{}
	}
	matches := 0
	for _, hit := range got {
		if _, ok := wanted[hit.Ref.DocID]; ok {
			matches++
		}
	}
	return float64(matches) / float64(len(want))
}
