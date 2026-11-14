package hnsw

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/internal/hnswmodel"
	"github.com/dariasmyr/fts-engine/v2/internal/memorystore"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

type forwardingPreparedStore struct {
	vector.PreparedVectorStore
}

type countingPreparedStore struct {
	vector.PreparedVectorStore
	reads int
}

type failingPreparedStore struct {
	vector.PreparedVectorStore
	err error
}

func (s failingPreparedStore) ReadVectorInto(
	context.Context,
	vector.Ordinal,
	[]float32,
) error {
	return s.err
}

func (s *countingPreparedStore) ReadVectorInto(
	ctx context.Context,
	ordinal vector.Ordinal,
	dst []float32,
) error {
	s.reads++
	return s.PreparedVectorStore.ReadVectorInto(ctx, ordinal, dst)
}

func TestBuildProducesDeterministicValidTopology(t *testing.T) {
	t.Parallel()

	values := make([][]float32, 32)
	for i := range values {
		values[i] = []float32{float32(i % 8), float32(i / 8)}
	}
	space, err := vector.NewSpace(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	store, err := memorystore.New(space, values)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{
		Build:  BuildConfig{MaxNeighbors: 4, EfConstruction: 32, Seed: 7},
		Search: SearchConfig{EfSearch: 16, VisitLimit: len(values)},
	}

	first, err := Build(context.Background(), store, config)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Build(context.Background(), store, config)
	if err != nil {
		t.Fatal(err)
	}
	firstSnapshot := Snapshot(first)
	if secondSnapshot := Snapshot(second); !reflect.DeepEqual(secondSnapshot, firstSnapshot) {
		t.Fatal("Build() topology differs for identical source, config, and seed")
	}
	assertTopologyInvariants(t, firstSnapshot)
}

func TestBuildMatchesReversePruningBaselineTopology(t *testing.T) {
	t.Parallel()

	const rows = 512
	values := make([][]float32, rows)
	for row := range rows {
		values[row] = []float32{
			float32((row*17)%101) / 101,
			float32((row*31)%103) / 103,
			float32((row*47)%107) / 107,
			float32((row*61)%109) / 109,
		}
	}
	space, err := vector.NewSpace(4, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	store, err := memorystore.New(space, values)
	if err != nil {
		t.Fatal(err)
	}
	index, err := Build(t.Context(), store, Config{
		Build:  BuildConfig{MaxNeighbors: 8, EfConstruction: 32, Seed: 7},
		Search: SearchConfig{EfSearch: 32, VisitLimit: rows},
	})
	if err != nil {
		t.Fatal(err)
	}

	fingerprint := sha256.Sum256([]byte(fmt.Sprintf("%#v", Snapshot(index))))
	const baseline = "dbad36cca1c64fa0877e70403b69f1ce1e2a05ae60d7d9b11db1430e7cfbda22"
	if got := fmt.Sprintf("%x", fingerprint); got != baseline {
		t.Fatalf("topology fingerprint = %s, want baseline %s", got, baseline)
	}
}

func TestBuildRecallAtTen(t *testing.T) {
	t.Parallel()

	const (
		rows       = 1_000
		dimensions = 32
		queries    = 50
	)
	values := make([]float32, rows*dimensions)
	randomState := uint64(1)
	for i := range values {
		randomState = randomState*6364136223846793005 + 1442695040888963407
		values[i] = float32(int32(randomState>>32)) / float32(1<<31)
	}
	space, err := vector.NewSpace(dimensions, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	store, err := memorystore.NewPrepared(space, values)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{
		Build:  BuildConfig{MaxNeighbors: 16, EfConstruction: 64, Seed: 1},
		Search: SearchConfig{EfSearch: 32, VisitLimit: rows},
	}
	index, err := Build(t.Context(), store, config)
	if err != nil {
		t.Fatal(err)
	}

	exact := make([][]nodeOrdinal, queries)
	for queryIndex := range queries {
		queryNode := queryIndex * (rows / queries)
		query := values[queryNode*dimensions : (queryNode+1)*dimensions]
		candidates := make([]candidate, rows)
		for row := range rows {
			candidates[row] = candidate{
				node:     nodeOrdinal(row),
				distance: space.DistancePrepared(query, values[row*dimensions:(row+1)*dimensions]),
			}
		}
		slices.SortFunc(candidates, func(a, c candidate) int {
			if candidateCloser(a, c) {
				return -1
			}
			if candidateCloser(c, a) {
				return 1
			}
			return 0
		})
		exact[queryIndex] = make([]nodeOrdinal, 10)
		for i := range 10 {
			exact[queryIndex][i] = candidates[i].node
		}
	}

	previousRecall := 0.0
	for _, efSearch := range []int{16, 32, 64, 200} {
		matches := 0
		for queryIndex := range queries {
			queryNode := queryIndex * (rows / queries)
			query := values[queryNode*dimensions : (queryNode+1)*dimensions]
			result, err := index.Search(
				t.Context(),
				query,
				10,
				QueryConfig{EfSearch: efSearch, VisitLimit: rows},
			)
			if err != nil {
				t.Fatal(err)
			}
			for _, hit := range result.Hits {
				if slices.Contains(exact[queryIndex], nodeOrdinal(hit.Ordinal)) {
					matches++
				}
			}
		}
		recall := float64(matches) / float64(queries*10)
		t.Logf("efSearch=%d recall@10=%.4f", efSearch, recall)
		if recall < previousRecall {
			t.Fatalf("recall decreased from %.4f to %.4f at efSearch=%d", previousRecall, recall, efSearch)
		}
		previousRecall = recall
	}
	if previousRecall < 0.99 {
		t.Fatalf("recall@10 at efSearch=200 = %.4f, want at least 0.99", previousRecall)
	}
}

func TestBuildCosineTopologyAndRecallAtTen(t *testing.T) {
	t.Parallel()

	const (
		rows       = 300
		dimensions = 768
		queries    = 20
	)
	values := make([]float32, rows*dimensions)
	randomState := uint64(1)
	for i := range values {
		randomState = randomState*6364136223846793005 + 1442695040888963407
		values[i] = float32(int32(randomState>>32)) / float32(1<<31)
	}
	space, err := vector.NewSpace(dimensions, vector.MetricCosine)
	if err != nil {
		t.Fatal(err)
	}
	for row := range rows {
		value := values[row*dimensions : (row+1)*dimensions]
		if err := space.PrepareInto(value, value); err != nil {
			t.Fatal(err)
		}
	}
	store, err := memorystore.NewPrepared(space, values)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{
		Build:  BuildConfig{MaxNeighbors: 16, EfConstruction: 64, Seed: 1},
		Search: SearchConfig{EfSearch: 64, VisitLimit: rows},
	}
	first, err := Build(t.Context(), store, config)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Build(t.Context(), store, config)
	if err != nil {
		t.Fatal(err)
	}
	firstSnapshot := Snapshot(first)
	secondSnapshot := Snapshot(second)
	if !reflect.DeepEqual(firstSnapshot, secondSnapshot) {
		t.Fatal("cosine builds with identical inputs and seed produced different topologies")
	}
	assertTopologyInvariants(t, firstSnapshot)
	fingerprint := sha256.Sum256([]byte(fmt.Sprintf("%#v", firstSnapshot)))
	const baseline = "71dfd2b5d7776b77a6b1078c34cc56d1a2c2e3b3c04fcc5d973ed13975f41040"
	if got := fmt.Sprintf("%x", fingerprint); got != baseline {
		t.Fatalf("cosine topology fingerprint = %s, want baseline %s", got, baseline)
	}

	matches := 0
	for queryIndex := range queries {
		queryNode := queryIndex * (rows / queries)
		query := values[queryNode*dimensions : (queryNode+1)*dimensions]
		candidates := make([]candidate, rows)
		for row := range rows {
			candidates[row] = candidate{
				node:     nodeOrdinal(row),
				distance: space.DistancePrepared(query, values[row*dimensions:(row+1)*dimensions]),
			}
		}
		slices.SortFunc(candidates, func(a, c candidate) int {
			if candidateCloser(a, c) {
				return -1
			}
			if candidateCloser(c, a) {
				return 1
			}
			return 0
		})
		result, err := first.Search(
			t.Context(),
			query,
			10,
			QueryConfig{EfSearch: 64, VisitLimit: rows},
		)
		if err != nil {
			t.Fatal(err)
		}
		for _, hit := range result.Hits {
			for _, exact := range candidates[:10] {
				if nodeOrdinal(hit.Ordinal) == exact.node {
					matches++
					break
				}
			}
		}
	}
	recall := float64(matches) / float64(queries*10)
	t.Logf("cosine recall@10=%.4f", recall)
	if recall < 0.99 {
		t.Fatalf("cosine recall@10 = %.4f, want at least 0.99", recall)
	}
}

func TestBuildBorrowedAndOwnedFallbackProduceIdenticalTopology(t *testing.T) {
	const (
		rows       = 128
		dimensions = 32
	)
	space, err := vector.NewSpace(dimensions, vector.MetricCosine)
	if err != nil {
		t.Fatal(err)
	}
	values := make([]float32, rows*dimensions)
	for row := range rows {
		value := values[row*dimensions : (row+1)*dimensions]
		for dimension := range dimensions {
			value[dimension] = float32(math.Sin(float64((row + 1) * (dimension + 1))))
		}
		if err := space.PrepareInto(value, value); err != nil {
			t.Fatal(err)
		}
	}
	wantValues := slices.Clone(values)
	borrowedStore, err := memorystore.NewPrepared(space, values)
	if err != nil {
		t.Fatal(err)
	}
	fallbackStore, err := memorystore.NewPrepared(space, slices.Clone(values))
	if err != nil {
		t.Fatal(err)
	}
	countingFallback := &countingPreparedStore{PreparedVectorStore: &forwardingPreparedStore{
		PreparedVectorStore: fallbackStore,
	}}
	config := Config{
		Build:  BuildConfig{MaxNeighbors: 16, EfConstruction: 64, Seed: 1},
		Search: SearchConfig{EfSearch: 32, VisitLimit: rows},
	}

	borrowedIndex, err := Build(t.Context(), borrowedStore, config)
	if err != nil {
		t.Fatal(err)
	}
	ownedIndex, err := Build(t.Context(), countingFallback, config)
	if err != nil {
		t.Fatal(err)
	}
	if countingFallback.reads != rows {
		t.Fatalf("owned fallback source reads = %d, want %d", countingFallback.reads, rows)
	}
	if !reflect.DeepEqual(Snapshot(borrowedIndex), Snapshot(ownedIndex)) {
		t.Fatal("borrowed and owned fallback builds produced different topologies")
	}
	borrowedValues, ok := memorystore.BorrowPreparedMatrix(borrowedStore)
	if !ok {
		t.Fatal("memory store did not expose its internal borrowed matrix")
	}
	if !slices.Equal(borrowedValues, wantValues) {
		t.Fatal("borrowed build modified the source matrix")
	}

	query := slices.Clone(wantValues[:dimensions])
	borrowedResult, err := borrowedIndex.Search(t.Context(), query, 10, QueryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ownedResult, err := ownedIndex.Search(t.Context(), query, 10, QueryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(borrowedResult, ownedResult) {
		t.Fatal("borrowed and owned fallback indexes produced different search results")
	}
}

func TestBuildOwnedFallbackPreservesReadErrors(t *testing.T) {
	space, err := vector.NewSpace(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	store, err := memorystore.NewPrepared(space, []float32{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("read failed")
	_, err = Build(t.Context(), failingPreparedStore{
		PreparedVectorStore: &forwardingPreparedStore{PreparedVectorStore: store},
		err:                 wantErr,
	}, Config{
		Build:  BuildConfig{MaxNeighbors: 2, EfConstruction: 2, Seed: 1},
		Search: SearchConfig{EfSearch: 1, VisitLimit: 1},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Build() error = %v, want wrapped read error", err)
	}
}

func TestBuildBorrowedPathStillValidatesPreparedRows(t *testing.T) {
	space, err := vector.NewSpace(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	store, err := memorystore.NewPrepared(space, []float32{1, float32(math.NaN())})
	if err != nil {
		t.Fatal(err)
	}
	_, err = Build(t.Context(), store, Config{
		Build:  BuildConfig{MaxNeighbors: 2, EfConstruction: 2, Seed: 1},
		Search: SearchConfig{EfSearch: 1, VisitLimit: 1},
	})
	if !errors.Is(err, vector.ErrNonFiniteVector) {
		t.Fatalf("Build() error = %v, want ErrNonFiniteVector", err)
	}
}

func TestBuildBorrowedMatrixLifetimeAndConcurrentReads(t *testing.T) {
	const (
		rows       = 512
		dimensions = 64
	)
	space, err := vector.NewSpace(dimensions, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	values := make([]float32, rows*dimensions)
	for i := range values {
		values[i] = float32(math.Sin(float64(i + 1)))
	}
	store, err := memorystore.NewPrepared(space, values)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{
		Build:  BuildConfig{MaxNeighbors: 16, EfConstruction: 64, Seed: 1},
		Search: SearchConfig{EfSearch: 32, VisitLimit: rows},
	}

	done := make(chan struct{})
	started := make(chan struct{})
	readErr := make(chan error, 1)
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		row := make([]float32, dimensions)
		firstRead := true
		for ordinal := 0; ; ordinal = (ordinal + 1) % rows {
			select {
			case <-done:
				return
			default:
			}
			if err := store.ReadVectorInto(t.Context(), vector.Ordinal(ordinal), row); err != nil {
				readErr <- err
				return
			}
			if firstRead {
				close(started)
				firstRead = false
			}
		}
	}()
	<-started
	index, buildErr := Build(t.Context(), store, config)
	close(done)
	readers.Wait()
	select {
	case err := <-readErr:
		t.Fatal(err)
	default:
	}
	if buildErr != nil {
		t.Fatal(buildErr)
	}

	store = nil
	values = nil
	runtime.GC()
	result, err := index.Search(t.Context(), make([]float32, dimensions), 10, QueryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 10 {
		t.Fatalf("Search() returned %d hits after source GC, want 10", len(result.Hits))
	}
}

func assertTopologyInvariants(t *testing.T, snapshot hnswmodel.Snapshot) {
	t.Helper()

	nodes := len(snapshot.Levels)
	if nodes == 0 || !snapshot.HasEntry || int(snapshot.Entry) >= nodes {
		t.Fatalf("entry = (%d, %t), nodes = %d", snapshot.Entry, snapshot.HasEntry, nodes)
	}
	if len(snapshot.Level0Offsets) != nodes+1 || len(snapshot.UpperNodeOffsets) != nodes+1 || len(snapshot.UpperLinkOffsets) == 0 {
		t.Fatalf(
			"offset lengths = level0:%d upper-nodes:%d upper-links:%d, nodes = %d",
			len(snapshot.Level0Offsets),
			len(snapshot.UpperNodeOffsets),
			len(snapshot.UpperLinkOffsets),
			nodes,
		)
	}
	if snapshot.Level0Offsets[0] != 0 || int(snapshot.Level0Offsets[nodes]) != len(snapshot.Level0Links) {
		t.Fatal("level-0 offsets do not cover level-0 links")
	}
	if snapshot.UpperNodeOffsets[0] != 0 || int(snapshot.UpperLinkOffsets[len(snapshot.UpperLinkOffsets)-1]) != len(snapshot.UpperLinks) {
		t.Fatal("upper offsets do not cover upper links")
	}

	maxLevel := uint8(0)
	placements := 0
	for node, level := range snapshot.Levels {
		maxLevel = max(maxLevel, level)
		if int(snapshot.UpperNodeOffsets[node]) != placements {
			t.Fatalf("node %d upper offset = %d, want %d", node, snapshot.UpperNodeOffsets[node], placements)
		}
		placements += int(level)
		assertNeighborList(t, snapshot, node, 0)
		for upperLevel := 1; upperLevel <= int(level); upperLevel++ {
			assertNeighborList(t, snapshot, node, upperLevel)
		}
	}
	if int(snapshot.UpperNodeOffsets[nodes]) != placements || len(snapshot.UpperLinkOffsets) != placements+1 {
		t.Fatalf("upper placements = %d, offsets = %d", placements, len(snapshot.UpperLinkOffsets))
	}
	if snapshot.Levels[snapshot.Entry] != maxLevel {
		t.Fatalf("entry level = %d, max level = %d", snapshot.Levels[snapshot.Entry], maxLevel)
	}
	assertLevel0Connected(t, snapshot)
}

func assertNeighborList(t *testing.T, snapshot hnswmodel.Snapshot, node, level int) {
	t.Helper()

	var neighbors []uint32
	limit := snapshot.MaxNeighbors
	if level == 0 {
		start := snapshot.Level0Offsets[node]
		end := snapshot.Level0Offsets[node+1]
		if end < start || int(end) > len(snapshot.Level0Links) {
			t.Fatalf("node %d has invalid level-0 range [%d:%d]", node, start, end)
		}
		neighbors = snapshot.Level0Links[start:end]
		limit = snapshot.LevelZeroMaxNeighbors
	} else {
		placement := snapshot.UpperNodeOffsets[node] + uint32(level-1)
		start := snapshot.UpperLinkOffsets[placement]
		end := snapshot.UpperLinkOffsets[placement+1]
		if end < start || int(end) > len(snapshot.UpperLinks) {
			t.Fatalf("node %d has invalid level-%d range [%d:%d]", node, level, start, end)
		}
		neighbors = snapshot.UpperLinks[start:end]
	}
	if len(neighbors) > limit {
		t.Fatalf("node %d level %d degree = %d, limit = %d", node, level, len(neighbors), limit)
	}
	seen := make(map[uint32]struct{}, len(neighbors))
	for _, neighbor := range neighbors {
		if int(neighbor) >= len(snapshot.Levels) || int(neighbor) == node || int(snapshot.Levels[neighbor]) < level {
			t.Fatalf("node %d level %d has invalid neighbor %d", node, level, neighbor)
		}
		if _, exists := seen[neighbor]; exists {
			t.Fatalf("node %d level %d has duplicate neighbor %d", node, level, neighbor)
		}
		seen[neighbor] = struct{}{}
	}
}

func assertLevel0Connected(t *testing.T, snapshot hnswmodel.Snapshot) {
	t.Helper()

	reachable := make([]bool, len(snapshot.Levels))
	reachable[snapshot.Entry] = true
	queue := []uint32{snapshot.Entry}
	visited := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		visited++
		start := snapshot.Level0Offsets[node]
		end := snapshot.Level0Offsets[node+1]
		for _, neighbor := range snapshot.Level0Links[start:end] {
			if !reachable[neighbor] {
				reachable[neighbor] = true
				queue = append(queue, neighbor)
			}
		}
	}
	if visited != len(snapshot.Levels) {
		t.Fatalf("level-0 reachable nodes = %d, want %d", visited, len(snapshot.Levels))
	}
}
