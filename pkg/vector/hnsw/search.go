package hnsw

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type searchCandidate struct {
	node     nodeOrdinal
	distance float64
	accepted bool
}

type searchState struct {
	ctx           context.Context
	index         *Index
	preparedQuery []float32
	reusableQuery *vector.PreparedQuery
	filter        vector.ResultFilter
	visitLimit    int
	workspace     *searchWorkspace
	stats         SearchStats
}

type searchRequest struct {
	k            int
	efSearch     int
	visitLimit   int
	allowedCount int
	filter       vector.ResultFilter
}

func resolveSearchRequest(config SearchConfig, vectorCount, k int, options SearchOptions) (searchRequest, error) {
	if k <= 0 {
		return searchRequest{}, vector.ErrInvalidK
	}
	if options.EfSearch < 0 || options.VisitLimit < 0 {
		return searchRequest{}, ErrInvalidSearchOptions
	}

	efSearch := options.EfSearch
	if efSearch == 0 {
		efSearch = config.EfSearch
	}
	efSearch = max(k, efSearch)
	if vectorCount > 0 {
		efSearch = min(efSearch, vectorCount)
	}

	visitLimit := options.VisitLimit
	if visitLimit == 0 {
		visitLimit = config.VisitLimit
	}
	if vectorCount > 0 {
		visitLimit = min(visitLimit, vectorCount)
	}

	allowedCount := vectorCount
	if options.ResultFilter != nil {
		if options.ResultFilter.TotalOrdinalCount() != uint32(vectorCount) {
			return searchRequest{}, fmt.Errorf("%w: got %d, want %d", vector.ErrResultFilterSizeMismatch, options.ResultFilter.TotalOrdinalCount(), vectorCount)
		}
		allowedCount = options.ResultFilter.AllowedOrdinalCount()
		if allowedCount < 0 || allowedCount > vectorCount {
			return searchRequest{}, ErrInvalidSearchOptions
		}
	}
	return searchRequest{k: k, efSearch: efSearch, visitLimit: visitLimit, allowedCount: allowedCount, filter: options.ResultFilter}, nil
}

func search(ctx context.Context, reader *Index, query []float32, prepared *vector.PreparedQuery, k int, options SearchOptions) (SearchResult, error) {
	if ctx == nil {
		return SearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	if reader == nil {
		return SearchResult{}, errInvalidGraph
	}
	calculator := reader.topology.calculator
	vectorCount := reader.Len()
	request, err := resolveSearchRequest(reader.search, vectorCount, k, options)
	if err != nil {
		return SearchResult{}, err
	}
	workspace := reader.workspaces.acquire()
	defer reader.workspaces.release(workspace)
	if prepared == nil {
		workspace.prepareQuery(calculator.Dimensions())
		if err := calculator.PrepareInto(workspace.preparedQuery, query); err != nil {
			return SearchResult{}, err
		}
	} else if err := calculator.ValidatePreparedQuery(*prepared); err != nil {
		return SearchResult{}, err
	}
	complete := SearchResult{Hits: []vector.Hit{}, Stats: SearchStats{Termination: TerminationComplete}}
	if vectorCount == 0 || request.allowedCount == 0 {
		if err := ctx.Err(); err != nil {
			return SearchResult{}, err
		}
		return complete, nil
	}
	workspace.resetSearch(vectorCount, calculator.Dimensions(), request.visitLimit, request.efSearch)
	entry, maxLevel, ok := reader.entryPoint()
	if !ok || maxLevel < 0 || maxLevel > maxLevelLimit || uint64(entry) >= uint64(vectorCount) {
		return SearchResult{}, errInvalidGraph
	}
	state := searchState{
		ctx: ctx, index: reader, preparedQuery: workspace.preparedQuery, filter: request.filter,
		reusableQuery: prepared,
		visitLimit:    request.visitLimit,
		workspace:     workspace,
		stats:         SearchStats{Termination: TerminationComplete},
	}
	entryCandidate, err := state.score(entry)
	if err != nil {
		return finishSearch(state, resultHeap{}, request.k, err)
	}
	// Sparse upper levels choose one local minimum; level 0 then widens into a beam.
	entryCandidate, err = greedySearch(&state, entryCandidate, maxLevel)
	if err != nil {
		return finishSearch(state, state.acceptedResults(min(request.efSearch, request.allowedCount)), request.k, err)
	}
	workspace.stopTrackingScored()
	results, err := levelSearch(&state, entryCandidate, request.efSearch, request.allowedCount)
	return finishSearch(state, results, request.k, err)
}

func greedySearch(state *searchState, current searchCandidate, maxLevel int) (searchCandidate, error) {
	for level := maxLevel; level > 0; level-- {
		for {
			if err := state.ctx.Err(); err != nil {
				return searchCandidate{}, err
			}
			state.stats.ExpandedNodes++
			best := current
			neighbors, ok := state.index.neighborView(current.node, level)
			if !ok {
				return searchCandidate{}, errInvalidGraph
			}
			for i, neighbor := range neighbors {
				if err := periodicContextError(state.ctx, i); err != nil {
					return searchCandidate{}, err
				}
				candidate, err := state.score(neighbor)
				if err != nil {
					return searchCandidate{}, err
				}
				if candidate.distance < current.distance && navigationBetter(candidate, best) {
					best = candidate
				}
			}
			if best.node == current.node {
				break
			}
			current = best
		}
	}
	return current, nil
}

func levelSearch(state *searchState, entry searchCandidate, efSearch, allowedCount int) (results resultHeap, err error) {
	resultCapacity := min(efSearch, allowedCount)
	results = newResultHeapWithBuffer(resultCapacity, state.workspace.results)
	frontier := candidateHeap{items: state.workspace.frontier}
	defer func() {
		state.workspace.results = results.items[:0]
		state.workspace.frontier = frontier.items[:0]
	}()
	state.workspace.markSeen(entry.node)
	frontier.Push(entry)
	if entry.accepted {
		results.Add(entry)
	}

	for frontier.Len() > 0 {
		if err := state.ctx.Err(); err != nil {
			return results, err
		}
		candidate, _ := frontier.Pop()

		// We do not stop the search when the result heap is full, because we need to continue expanding the frontier to ensure that we have found the best candidates.
		// However, we can stop expanding the frontier if the candidate's distance is greater than the worst distance in the results heap and we have already filled the results heap to capacity.
		if results.Len() >= resultCapacity {
			worst, _ := results.Worst()

			if candidate.distance > worst.distance {
				break
			}
		}
		if worst, ok := results.Worst(); ok && results.Len() >= efSearch && candidate.distance > worst.distance {
			break
		}
		state.stats.ExpandedNodes++
		neighbors, ok := state.index.neighborView(candidate.node, 0)
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

			if results.Len() >= resultCapacity {
				worst, _ := results.Worst()
				if discovered.distance > worst.distance {
					continue
				}
			}

			worst, full := results.Worst()
			if !full || results.Len() < efSearch || discovered.distance <= worst.distance {
				frontier.Push(discovered)
			}
		}
	}
	return results, nil
}

func (state *searchState) score(node nodeOrdinal) (searchCandidate, error) {
	if uint64(node) >= uint64(state.index.Len()) {
		return searchCandidate{}, errInvalidGraph
	}
	if candidate, exists := state.workspace.candidate(node); exists {
		return candidate, nil
	}
	// VisitedNodes and the visit budget count unique query-to-node scores across
	// all levels. The cache prevents duplicate distance work through converging paths.
	if state.stats.VisitedNodes >= state.visitLimit {
		return searchCandidate{}, errVisitLimit
	}
	ordinal := vector.Ordinal(node)
	value := state.workspace.vector
	if err := state.index.vectors.ReadVectorInto(state.ctx, ordinal, value); err != nil {
		if state.ctx.Err() != nil {
			return searchCandidate{}, state.ctx.Err()
		}
		return searchCandidate{}, fmt.Errorf("%w: read vector row %d: %v", errInvalidGraph, ordinal, err)
	}
	var distance float64
	if state.reusableQuery != nil {
		distance = state.index.topology.calculator.DistancePreparedQuery(*state.reusableQuery, value)
	} else {
		distance = state.index.topology.calculator.DistancePrepared(state.preparedQuery, value)
	}
	if math.IsNaN(distance) || math.IsInf(distance, 0) {
		return searchCandidate{}, errInvalidGraph
	}
	accepted := state.filter == nil || state.filter.Allows(ordinal)
	candidate := searchCandidate{node: node, distance: distance, accepted: accepted}
	state.workspace.storeCandidate(candidate)
	state.stats.VisitedNodes++
	state.stats.DistanceComputations++
	if !accepted {
		state.stats.RejectedNodes++
	}
	return candidate, nil
}

func (state *searchState) acceptedResults(capacity int) resultHeap {
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

func finishSearch(state searchState, results resultHeap, k int, err error) (SearchResult, error) {
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
