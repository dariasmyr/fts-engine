package hnsw

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

type candidate struct {
	node     nodeOrdinal
	distance float64
	accepted bool
}

type state struct {
	ctx           context.Context
	index         *Index
	preparedQuery []float32
	reusableQuery *vector.PreparedVector
	filter        vector.ResultFilter
	visitLimit    int
	workspace     *searchWorkspace
	stats         SearchStats
}

type request struct {
	k                  int
	configuredEfSearch int
	appliedEfSearch    int
	visitLimit         int
	allowedCount       int
	filter             vector.ResultFilter
}

func resolveSearchRequest(defaults SearchConfig, vectorCount, k int, config QueryConfig) (request, error) {
	if k <= 0 {
		return request{}, vector.ErrInvalidK
	}
	if err := config.Validate(); err != nil {
		return request{}, err
	}

	configuredEfSearch := config.EfSearch
	if configuredEfSearch == 0 {
		configuredEfSearch = defaults.EfSearch
	}
	appliedEfSearch := max(k, configuredEfSearch)
	if vectorCount > 0 {
		appliedEfSearch = min(appliedEfSearch, vectorCount)
	} else {
		appliedEfSearch = 0
	}

	visitLimit := config.VisitLimit
	if visitLimit == 0 {
		visitLimit = defaults.VisitLimit
	}
	if vectorCount > 0 {
		visitLimit = min(visitLimit, vectorCount)
	}

	allowedCount := vectorCount
	if config.ResultFilter != nil {
		if config.ResultFilter.TotalOrdinalCount() != uint32(vectorCount) {
			return request{}, fmt.Errorf("%w: got %d, want %d", vector.ErrResultFilterSizeMismatch, config.ResultFilter.TotalOrdinalCount(), vectorCount)
		}
		allowedCount = config.ResultFilter.AllowedOrdinalCount()
		if allowedCount < 0 || allowedCount > vectorCount {
			return request{}, ErrInvalidQueryConfig
		}
	}
	return request{
		k: k, configuredEfSearch: configuredEfSearch, appliedEfSearch: appliedEfSearch,
		visitLimit: visitLimit, allowedCount: allowedCount, filter: config.ResultFilter,
	}, nil
}

func search(ctx context.Context, reader *Index, query []float32, prepared *vector.PreparedVector, k int, config QueryConfig) (SearchResult, error) {
	if ctx == nil {
		return SearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	if reader == nil {
		return SearchResult{}, errInvalidGraph
	}
	space := reader.topology.space
	vectorCount := reader.Len()
	request, err := resolveSearchRequest(reader.config.Search, vectorCount, k, config)
	if err != nil {
		return SearchResult{}, err
	}
	workspace := reader.workspaces.acquire()
	defer reader.workspaces.release(workspace)
	if prepared == nil {
		workspace.prepareQuery(space.Dimensions())
		if err := space.PrepareInto(workspace.preparedQuery, query); err != nil {
			return SearchResult{}, err
		}
	} else if err := space.ValidatePreparedVector(*prepared); err != nil {
		return SearchResult{}, err
	}
	stats := SearchStats{
		ConfiguredEfSearch: request.configuredEfSearch,
		AppliedEfSearch:    request.appliedEfSearch,
		Termination:        TerminationComplete,
	}
	complete := SearchResult{Hits: []vector.Hit{}, Stats: stats}
	if vectorCount == 0 || request.allowedCount == 0 {
		if err := ctx.Err(); err != nil {
			return SearchResult{}, err
		}
		return complete, nil
	}
	workspace.resetSearch(vectorCount, space.Dimensions(), request.visitLimit, request.appliedEfSearch)
	entry, maxLevel, ok := reader.entryPoint()
	if !ok || maxLevel < 0 || maxLevel > maxLevelLimit || uint64(entry) >= uint64(vectorCount) {
		return SearchResult{}, errInvalidGraph
	}
	state := state{
		ctx: ctx, index: reader, preparedQuery: workspace.preparedQuery, filter: request.filter,
		reusableQuery: prepared,
		visitLimit:    request.visitLimit,
		workspace:     workspace,
		stats:         stats,
	}
	entryCandidate, err := state.score(entry)
	if err != nil {
		return finishSearch(state, resultHeap{}, request.k, err)
	}
	// Sparse upper levels choose one local minimum; level 0 then widens into a beam.
	entryCandidate, err = greedySearch(&state, entryCandidate, maxLevel)
	if err != nil {
		return finishSearch(state, state.acceptedResults(min(request.appliedEfSearch, request.allowedCount)), request.k, err)
	}
	workspace.stopTrackingScored()
	results, err := levelSearch(&state, entryCandidate, request.appliedEfSearch, request.allowedCount)
	return finishSearch(state, results, request.k, err)
}

func greedySearch(state *state, currentCandidate candidate, maxLevel int) (candidate, error) {
	for level := maxLevel; level > 0; level-- {
		for {
			if err := state.ctx.Err(); err != nil {
				return candidate{}, err
			}
			state.stats.ExpandedNodes++
			best := currentCandidate
			neighbors, ok := state.index.neighborView(currentCandidate.node, level)
			if !ok {
				return candidate{}, errInvalidGraph
			}
			for i, neighbor := range neighbors {
				if err := periodicContextError(state.ctx, i); err != nil {
					return candidate{}, err
				}
				candidateScore, err := state.score(neighbor)
				if err != nil {
					return candidate{}, err
				}
				if candidateScore.distance < currentCandidate.distance && candidateCloser(candidateScore, best) {
					best = candidateScore
				}
			}
			if best.node == currentCandidate.node {
				break
			}
			currentCandidate = best
		}
	}
	return currentCandidate, nil
}

func levelSearch(
	state *state,
	entry candidate,
	efSearch, allowedCount int,
) (results resultHeap, err error) {
	resultCapacity := min(efSearch, allowedCount)

	results = newResultHeapWithBuffer(
		resultCapacity,
		state.workspace.results,
	)

	candidates := candidateHeap{
		items: state.workspace.frontier,
	}

	defer func() {
		state.workspace.results = results.items[:0]
		state.workspace.frontier = candidates.items[:0]
	}()

	state.workspace.markSeen(entry.node)
	candidates.Push(entry)

	if entry.accepted {
		results.Add(entry)
	}

	for candidates.Len() > 0 {
		if err := state.ctx.Err(); err != nil {
			return results, err
		}

		candidate, _ := candidates.Pop()

		// Все разрешённые вершины уже найдены.
		if results.Len() == allowedCount {
			break
		}

		// Эвристическая остановка Beam Search.
		if results.Len() >= efSearch {
			worst, _ := results.Worst()

			if candidate.distance > worst.distance {
				break
			}
		}

		state.stats.ExpandedNodes++

		neighbors, ok := state.index.neighborView(
			candidate.node,
			0,
		)
		if !ok {
			return results, errInvalidGraph
		}

		for i, neighbor := range neighbors {
			if err := periodicContextError(state.ctx, i); err != nil {
				return results, err
			}

			if uint64(neighbor) >= uint64(state.index.Len()) {
				return results, errInvalidGraph
			}

			if !state.workspace.markSeen(neighbor) {
				continue
			}

			discovered, err := state.score(neighbor)
			if err != nil {
				return results, err
			}

			if discovered.accepted {
				results.Add(discovered)
			}

			// All living (allowed by the result filter)
			// vectors have been found, no need to continue searching.
			if results.Len() == allowedCount {
				continue
			}

			// Add the discovered node to the search frontier if it is not worse than the worst result.
			worst, full := results.Worst()

			if !full ||
				results.Len() < efSearch ||
				discovered.distance <= worst.distance {
				candidates.Push(discovered)
			}
		}
	}

	return results, nil
}

func (state *state) score(node nodeOrdinal) (candidate, error) {
	if uint64(node) >= uint64(state.index.Len()) {
		return candidate{}, errInvalidGraph
	}
	if candidate, exists := state.workspace.candidate(node); exists {
		return candidate, nil
	}
	// VisitedNodes and the visit budget count unique query-to-node scores across
	// all levels. The cache prevents duplicate distance work through converging paths.
	if state.stats.VisitedNodes >= state.visitLimit {
		return candidate{}, errVisitLimit
	}
	ordinal := vector.Ordinal(node)
	value := state.workspace.vector
	if err := state.index.vectors.ReadVectorInto(state.ctx, ordinal, value); err != nil {
		if state.ctx.Err() != nil {
			return candidate{}, state.ctx.Err()
		}
		return candidate{}, fmt.Errorf("%w: read vector row %d: %v", errInvalidGraph, ordinal, err)
	}
	var distance float64
	if state.reusableQuery != nil {
		distance = state.index.topology.space.DistancePreparedVector(*state.reusableQuery, value)
	} else {
		distance = state.index.topology.space.DistancePrepared(state.preparedQuery, value)
	}
	if math.IsNaN(distance) || math.IsInf(distance, 0) {
		return candidate{}, errInvalidGraph
	}
	accepted := state.filter == nil || state.filter.Allows(ordinal)
	candidate := candidate{node: node, distance: distance, accepted: accepted}
	state.workspace.storeCandidate(candidate)
	state.stats.VisitedNodes++
	state.stats.DistanceComputations++
	if !accepted {
		state.stats.RejectedNodes++
	}
	return candidate, nil
}

func (state *state) acceptedResults(capacity int) resultHeap {
	results := newResultHeapWithBuffer(capacity, state.workspace.results)
	for _, node := range state.workspace.scoredNodes {
		candidate, _ := state.workspace.candidate(node)
		if candidate.accepted {
			results.Add(candidate)
		}
	}
	state.workspace.results = results.items[:0]
	return results
}

func periodicContextError(ctx context.Context, iteration int) error {
	if iteration&63 != 0 {
		return nil
	}
	return ctx.Err()
}

func finishSearch(state state, results resultHeap, k int, err error) (SearchResult, error) {
	if err != nil && !errors.Is(err, errVisitLimit) {
		return SearchResult{}, err
	}
	if contextErr := state.ctx.Err(); contextErr != nil {
		return SearchResult{}, contextErr
	}
	result := SearchResult{Hits: results.Hits(k), Stats: state.stats}
	if errors.Is(err, errVisitLimit) {
		result.Incomplete = true
		result.Stats.Termination = TerminationVisitLimit
	}
	return result, nil
}
