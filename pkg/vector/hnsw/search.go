package hnsw

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/dariasmyr/fts-engine/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

var errVisitLimit = errors.New("vector/hnsw: visit limit reached")

type searchState struct {
	ctx           context.Context
	index         *Index
	preparedQuery []float32
	reusableQuery *vector.PreparedQuery
	filter        vector.ResultFilter
	visitLimit    int
	workspace     *searchWorkspace
	stats         vector.SearchStats
}

type searchRequest struct {
	k            int
	efSearch     int
	visitLimit   int
	allowedCount int
	filter       vector.ResultFilter
}

func resolveSearchRequest(config SearchConfig, vectorCount, k int, options vector.SearchOptions) (searchRequest, error) {
	if k <= 0 || k > config.MaxK {
		return searchRequest{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, config.MaxK)
	}
	if options.EfSearch < 0 || options.VisitLimit < 0 {
		return searchRequest{}, vector.ErrInvalidSearchOptions
	}
	efSearch := options.EfSearch
	if efSearch == 0 {
		efSearch = config.DefaultEfSearch
	}
	efSearch = max(k, efSearch)
	if efSearch > config.MaxEfSearch {
		return searchRequest{}, fmt.Errorf("%w: efSearch=%d max=%d", vector.ErrInvalidSearchOptions, efSearch, config.MaxEfSearch)
	}
	visitLimit := options.VisitLimit
	if visitLimit == 0 {
		visitLimit = config.DefaultVisitLimit
	}
	if visitLimit > config.MaxVisitLimit {
		return searchRequest{}, fmt.Errorf("%w: visitLimit=%d max=%d", vector.ErrInvalidSearchOptions, visitLimit, config.MaxVisitLimit)
	}
	allowedCount := vectorCount
	if options.ResultFilter != nil {
		if options.ResultFilter.TotalOrdinalCount() != uint32(vectorCount) {
			return searchRequest{}, fmt.Errorf("%w: got %d, want %d", vector.ErrResultFilterSizeMismatch, options.ResultFilter.TotalOrdinalCount(), vectorCount)
		}
		allowedCount = options.ResultFilter.AllowedOrdinalCount()
		if allowedCount < 0 || allowedCount > vectorCount {
			return searchRequest{}, vector.ErrInvalidSearchOptions
		}
	}
	return searchRequest{k: k, efSearch: efSearch, visitLimit: visitLimit, allowedCount: allowedCount, filter: options.ResultFilter}, nil
}

func search(ctx context.Context, reader *Index, query []float32, prepared *vector.PreparedQuery, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	if ctx == nil {
		return vector.SearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return vector.SearchResult{}, err
	}
	if reader == nil {
		return vector.SearchResult{}, errInvalidGraph
	}
	calculator := reader.topology.calculator
	vectorCount := reader.Len()
	request, err := resolveSearchRequest(reader.search, vectorCount, k, options)
	if err != nil {
		return vector.SearchResult{}, err
	}
	workspace := reader.workspaces.acquire()
	defer reader.workspaces.release(workspace)
	if prepared == nil {
		workspace.prepareQuery(calculator.Dimensions())
		if err := calculator.PrepareInto(workspace.preparedQuery, query); err != nil {
			return vector.SearchResult{}, err
		}
	} else if err := calculator.ValidatePreparedQuery(*prepared); err != nil {
		return vector.SearchResult{}, err
	}
	complete := vector.SearchResult{Hits: []vector.Hit{}, Stats: vector.SearchStats{Termination: vector.TerminationComplete}}
	if vectorCount == 0 || request.allowedCount == 0 {
		if err := ctx.Err(); err != nil {
			return vector.SearchResult{}, err
		}
		return complete, nil
	}
	workspace.resetSearch(vectorCount, calculator.Dimensions(), request.visitLimit, request.efSearch)
	entry, maxLevel, ok := reader.entryPoint()
	if !ok || maxLevel < 0 || maxLevel > MaxLevel || uint64(entry) >= uint64(vectorCount) {
		return vector.SearchResult{}, errInvalidGraph
	}
	state := searchState{
		ctx: ctx, index: reader, preparedQuery: workspace.preparedQuery, filter: request.filter,
		reusableQuery: prepared,
		visitLimit:    request.visitLimit,
		workspace:     workspace,
		stats:         vector.SearchStats{Termination: vector.TerminationComplete},
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
				if err := contextcheck.PeriodicError(state.ctx, i); err != nil {
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
		if results.Len() == allowedCount {
			break
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
			if err := contextcheck.PeriodicError(state.ctx, i); err != nil {
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
			if results.Len() == allowedCount {
				continue
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

func finishSearch(state searchState, results resultHeap, k int, err error) (vector.SearchResult, error) {
	if err != nil && !errors.Is(err, errVisitLimit) {
		return vector.SearchResult{}, err
	}
	if contextErr := state.ctx.Err(); contextErr != nil {
		return vector.SearchResult{}, contextErr
	}
	result := vector.SearchResult{Hits: results.Hits(k), Stats: state.stats}
	if errors.Is(err, errVisitLimit) {
		result.Incomplete = true
		result.Stats.Termination = vector.TerminationVisitLimit
	}
	return result, nil
}
