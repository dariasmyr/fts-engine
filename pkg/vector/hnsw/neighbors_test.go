package hnsw

import (
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestNeighborSelectionUsesFallbackOnlyForDirectLinks(t *testing.T) {
	t.Parallel()

	b := newConnectivityTestBuilder(
		t,
		[]float32{0, 1, 2, 3, 4, 5},
		[][]nodeOrdinal{nil, nil, nil, nil, nil, nil},
		2,
	)
	candidates := make([]candidate, 0, 5)
	for node := nodeOrdinal(1); node <= 5; node++ {
		candidates = append(candidates, candidate{
			node:     node,
			distance: b.distanceNodes(0, node),
		})
	}

	direct, err := b.selectClosestNeighbors(t.Context(), candidates, 4)
	if err != nil {
		t.Fatal(err)
	}
	if want := []nodeOrdinal{1, 2, 3, 4}; !slices.Equal(neighborOrdinals(direct), want) {
		t.Fatalf("direct links = %v, want fallback-filled %v", direct, want)
	}

	b.graph.nodes[0].links[0] = buildNeighborsFromOrdinals(b, 0, []nodeOrdinal{1, 2, 3, 4})
	if err := b.addReverseLink(t.Context(), 0, 5, 0); err != nil {
		t.Fatal(err)
	}
	if want := []nodeOrdinal{1}; !slices.Equal(neighborOrdinals(b.graph.nodes[0].links[0]), want) {
		t.Fatalf("reverse links = %v, want diversified-only %v", b.graph.nodes[0].links[0], want)
	}
}

func TestIncrementalNeighborSelectionMatchesFullRecomputation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		values           [][]float32
		candidates       []candidate
		limit            int
		fillFromRejected bool
	}{
		{
			name:   "verified candidates and new candidate",
			values: [][]float32{{0, 0}, {1, 0}, {-1, 0}, {0, 1}, {0, -1}, {2, 0}},
			candidates: []candidate{
				{node: 1, verified: true},
				{node: 2, verified: true},
				{node: 3, verified: true},
				{node: 4, verified: true},
				{node: 5},
			},
			limit: 4,
		},
		{
			name:   "fallback candidates stay unverified",
			values: [][]float32{{0}, {1}, {2}, {3}, {4}, {5}},
			candidates: []candidate{
				{node: 1, verified: true},
				{node: 2},
				{node: 3},
				{node: 4},
				{node: 5},
			},
			limit:            4,
			fillFromRejected: true,
		},
		{
			name:   "duplicate candidate invalidates reuse",
			values: [][]float32{{0}, {1}, {2}, {3}},
			candidates: []candidate{
				{node: 1, verified: true},
				{node: 1},
				{node: 2},
				{node: 3},
			},
			limit: 3,
		},
		{
			name:   "equal target distances keep ordinal tie breaking",
			values: [][]float32{{0, 0}, {1, 0}, {-1, 0}, {0, 1}, {0, -1}, {1, 0}},
			candidates: []candidate{
				{node: 4, verified: true},
				{node: 3, verified: true},
				{node: 2, verified: true},
				{node: 1, verified: true},
				{node: 5},
			},
			limit:            4,
			fillFromRejected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newNeighborSelectionTestBuilder(t, tt.values, 4)
			incrementalCandidates := slices.Clone(tt.candidates)
			for i := range incrementalCandidates {
				incrementalCandidates[i].distance = b.distanceNodes(0, incrementalCandidates[i].node)
			}
			fullCandidates := slices.Clone(incrementalCandidates)
			for i := range fullCandidates {
				fullCandidates[i].verified = false
			}

			incremental, err := b.selectNeighbors(t.Context(), incrementalCandidates, tt.limit, tt.fillFromRejected)
			if err != nil {
				t.Fatal(err)
			}
			full, err := b.selectNeighbors(t.Context(), fullCandidates, tt.limit, tt.fillFromRejected)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(incremental.neighbors, full.neighbors) {
				t.Fatalf("incremental neighbors = %v, full recomputation = %v", incremental.neighbors, full.neighbors)
			}
			if incremental.verifiedCount != full.verifiedCount {
				t.Fatalf("incremental verified = %d, full recomputation = %d", incremental.verifiedCount, full.verifiedCount)
			}
			if incremental.distanceChecks > full.distanceChecks {
				t.Fatalf("incremental checks = %d, full checks = %d", incremental.distanceChecks, full.distanceChecks)
			}
		})
	}
}

func TestIncrementalNeighborSelectionSkipsConfirmedPairs(t *testing.T) {
	t.Parallel()

	b := newNeighborSelectionTestBuilder(
		t,
		[][]float32{{0, 0}, {1, 0}, {-1, 0}, {0, 1}, {0, -1}, {2, 0}},
		4,
	)
	candidates := []candidate{
		{node: 1, distance: b.distanceNodes(0, 1), verified: true},
		{node: 2, distance: b.distanceNodes(0, 2), verified: true},
		{node: 3, distance: b.distanceNodes(0, 3), verified: true},
		{node: 4, distance: b.distanceNodes(0, 4), verified: true},
		{node: 5, distance: b.distanceNodes(0, 5)},
	}
	selection, err := b.selectNeighbors(t.Context(), candidates, 4, false)
	if err != nil {
		t.Fatal(err)
	}
	if selection.skippedChecks == 0 {
		t.Fatal("incremental selection skipped no confirmed comparisons")
	}
	fullCandidates := slices.Clone(candidates)
	for i := range fullCandidates {
		fullCandidates[i].verified = false
	}
	full, err := b.selectNeighbors(t.Context(), fullCandidates, 4, false)
	if err != nil {
		t.Fatal(err)
	}
	if selection.distanceChecks >= full.distanceChecks {
		t.Fatalf("incremental checks = %d, full checks = %d", selection.distanceChecks, full.distanceChecks)
	}
}

func TestVerifiedLinkBitsSupportUpperLevelsAndLargeDegrees(t *testing.T) {
	t.Parallel()

	b := newNeighborSelectionTestBuilder(t, [][]float32{{0}}, 65)
	b.graph.nodes[0].level = 1
	b.graph.nodes[0].links = make([][]buildNeighbor, 2)
	b.setVerifiedPrefix(0, 0, 130)
	b.setVerifiedPrefix(0, 1, 65)

	for _, test := range []struct {
		level int
		index int
		want  bool
	}{
		{level: 0, index: 0, want: true},
		{level: 0, index: 129, want: true},
		{level: 0, index: 130, want: false},
		{level: 1, index: 64, want: true},
		{level: 1, index: 65, want: false},
	} {
		if got := b.linkVerified(0, test.level, test.index); got != test.want {
			t.Fatalf("level %d index %d verified = %t, want %t", test.level, test.index, got, test.want)
		}
	}
}

func newNeighborSelectionTestBuilder(t testing.TB, values [][]float32, maxNeighbors int) *builder {
	t.Helper()
	if len(values) == 0 {
		t.Fatal("neighbor selection test requires a target vector")
	}
	space, err := vector.NewSpace(len(values[0]), vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	b := newBuilder(
		BuildConfig{MaxNeighbors: maxNeighbors, EfConstruction: maxNeighbors, Seed: 1},
		space,
		len(values),
		len(values)*space.Dimensions(),
	)
	for _, value := range values {
		if len(value) != space.Dimensions() {
			t.Fatal("inconsistent test vector dimensions")
		}
		copy(b.graph.values[len(b.graph.nodes)*space.Dimensions():], value)
		b.graph.nodes = append(b.graph.nodes, mutableNode{links: [][]buildNeighbor{nil}})
	}
	return b
}
