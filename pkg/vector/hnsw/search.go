package hnsw

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/internal/contextcheck"
)

var errVisitLimit = errors.New("vector/hnsw: visit limit reached")

type searchState struct {
	ctx           context.Context
	index         *Searcher
	calculator    vector.Calculator
	preparedQuery []float32
	filter        vector.ResultFilter
	visitLimit    int
	vectorCount   int
	candidates    map[NodeOrdinal]searchCandidate
	workItems     int
	stats         vector.SearchStats
}

func search(ctx context.Context, reader *Searcher, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	if ctx == nil {
		return vector.SearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return vector.SearchResult{}, err
	}
	if reader == nil || !reader.topology.validated {
		return vector.SearchResult{}, ErrInvalidGraph
	}
	config := reader.topology.searchConfig
	calculator := reader.topology.calculator
	if err := config.validate(); err != nil {
		return vector.SearchResult{}, err
	}
	if k <= 0 || k > config.MaxK {
		return vector.SearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, config.MaxK)
	}
	if options.EfSearch < 0 || options.VisitLimit < 0 {
		return vector.SearchResult{}, vector.ErrInvalidSearchOptions
	}
	efSearch := options.EfSearch
	if efSearch == 0 {
		efSearch = config.DefaultEfSearch
	}
	efSearch = max(k, efSearch)
	if efSearch > config.MaxEfSearch {
		return vector.SearchResult{}, fmt.Errorf("%w: efSearch=%d max=%d", vector.ErrInvalidSearchOptions, efSearch, config.MaxEfSearch)
	}
	visitLimit := options.VisitLimit
	if visitLimit == 0 {
		visitLimit = config.DefaultVisitLimit
	}
	if visitLimit > config.MaxVisitLimit {
		return vector.SearchResult{}, fmt.Errorf("%w: visitLimit=%d max=%d", vector.ErrInvalidSearchOptions, visitLimit, config.MaxVisitLimit)
	}
	preparedQuery, err := calculator.Prepare(query)
	if err != nil {
		return vector.SearchResult{}, err
	}
	vectorCount := reader.Len()
	nodeCount := reader.NodeCount()
	filter := options.ResultFilter
	allowedCount := vectorCount
	if filter != nil {
		if filter.TotalOrdinalCount() != uint32(vectorCount) {
			return vector.SearchResult{}, fmt.Errorf("%w: got %d, want %d", vector.ErrResultFilterSizeMismatch, filter.TotalOrdinalCount(), vectorCount)
		}
		allowedCount = filter.AllowedOrdinalCount()
		if allowedCount < 0 || allowedCount > vectorCount {
			return vector.SearchResult{}, vector.ErrInvalidSearchOptions
		}
	}
	complete := vector.SearchResult{Hits: []vector.Hit{}, Stats: vector.SearchStats{Termination: vector.TerminationComplete}}
	if nodeCount == 0 || vectorCount == 0 {
		if nodeCount != 0 || vectorCount != 0 {
			return vector.SearchResult{}, ErrInvalidGraph
		}
		if err := ctx.Err(); err != nil {
			return vector.SearchResult{}, err
		}
		return complete, nil
	}
	if allowedCount == 0 {
		if err := ctx.Err(); err != nil {
			return vector.SearchResult{}, err
		}
		return complete, nil
	}
	entry, maxLevel, ok := reader.EntryPoint()
	if !ok || maxLevel < 0 || maxLevel > MaxLevel || uint64(entry) >= uint64(nodeCount) {
		return vector.SearchResult{}, ErrInvalidGraph
	}
	state := searchState{
		ctx: ctx, index: reader, calculator: calculator, preparedQuery: preparedQuery, filter: filter,
		visitLimit: visitLimit, vectorCount: vectorCount,
		candidates: make(map[NodeOrdinal]searchCandidate, min(visitLimit, nodeCount)),
		stats:      vector.SearchStats{Termination: vector.TerminationComplete},
	}
	entryCandidate, err := state.score(entry)
	if err != nil {
		return finishSearch(state, resultHeap{}, k, err)
	}
	// Sparse upper levels choose one local minimum; level 0 then widens into a beam.
	entryCandidate, err = greedySearch(&state, entryCandidate, maxLevel)
	if err != nil {
		return finishSearch(state, state.acceptedResults(min(efSearch, allowedCount)), k, err)
	}
	results, err := levelSearch(&state, entryCandidate, efSearch, allowedCount)
	return finishSearch(state, results, k, err)
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
				return searchCandidate{}, ErrInvalidGraph
			}
			for _, neighbor := range neighbors {
				if err := state.periodicContextError(); err != nil {
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

func levelSearch(state *searchState, entry searchCandidate, efSearch, allowedCount int) (resultHeap, error) {
	resultCapacity := min(efSearch, allowedCount)
	results := newResultHeap(resultCapacity)
	frontier := candidateHeap{items: make([]searchCandidate, 0, min(efSearch, state.index.NodeCount()))}
	seen := make(map[NodeOrdinal]struct{}, min(state.visitLimit, state.index.NodeCount()))
	seen[entry.node] = struct{}{}
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
			return results, ErrInvalidGraph
		}
		for _, neighbor := range neighbors {
			if err := state.periodicContextError(); err != nil {
				return results, err
			}
			if uint64(neighbor) >= uint64(state.index.NodeCount()) {
				return results, ErrInvalidGraph
			}
			if _, exists := seen[neighbor]; exists {
				continue
			}
			seen[neighbor] = struct{}{}
			discovered, err := state.score(neighbor)
			if err != nil {
				return results, err
			}
			frontier.Push(discovered)
			if discovered.accepted {
				results.Add(discovered)
			}
		}
	}
	return results, nil
}

func (state *searchState) score(node NodeOrdinal) (searchCandidate, error) {
	if uint64(node) >= uint64(state.index.NodeCount()) {
		return searchCandidate{}, ErrInvalidGraph
	}
	if candidate, exists := state.candidates[node]; exists {
		return candidate, nil
	}
	// VisitedNodes and the visit budget count unique query-to-node scores across
	// all levels. The cache prevents duplicate distance work through converging paths.
	if state.stats.VisitedNodes >= state.visitLimit {
		return searchCandidate{}, errVisitLimit
	}
	if uint64(node) >= uint64(len(state.index.topology.nodeToVector)) {
		return searchCandidate{}, ErrInvalidGraph
	}
	ordinal := state.index.topology.nodeToVector[node]
	if uint64(ordinal) >= uint64(state.vectorCount) {
		return searchCandidate{}, ErrInvalidGraph
	}
	value := make([]float32, state.calculator.Dimensions())
	if err := state.index.VectorSource().ReadVectorInto(state.ctx, ordinal, value); err != nil {
		if state.ctx.Err() != nil {
			return searchCandidate{}, state.ctx.Err()
		}
		return searchCandidate{}, fmt.Errorf("%w: read vector row %d: %v", ErrInvalidGraph, ordinal, err)
	}
	distance := state.calculator.DistancePrepared(state.preparedQuery, value)
	if math.IsNaN(distance) || math.IsInf(distance, 0) {
		return searchCandidate{}, ErrInvalidGraph
	}
	accepted := state.filter == nil || state.filter.Allows(ordinal)
	candidate := searchCandidate{node: node, vectorOrdinal: ordinal, distance: distance, accepted: accepted}
	state.candidates[node] = candidate
	state.stats.VisitedNodes++
	state.stats.DistanceComputations++
	if !accepted {
		state.stats.RejectedNodes++
	}
	return candidate, nil
}

func (state *searchState) periodicContextError() error {
	err := contextcheck.PeriodicError(state.ctx, state.workItems)
	state.workItems++
	return err
}

func (state *searchState) acceptedResults(capacity int) resultHeap {
	results := newResultHeap(capacity)
	for _, candidate := range state.candidates {
		if candidate.accepted {
			results.Add(candidate)
		}
	}
	return results
}

func finishSearch(state searchState, results resultHeap, k int, err error) (vector.SearchResult, error) {
	if err != nil && !errors.Is(err, errVisitLimit) {
		return vector.SearchResult{}, err
	}
	if contextErr := state.ctx.Err(); contextErr != nil {
		return vector.SearchResult{}, contextErr
	}
	result := vector.SearchResult{Hits: results.Results(k), Stats: state.stats}
	if errors.Is(err, errVisitLimit) {
		result.Incomplete = true
		result.Stats.Termination = vector.TerminationVisitLimit
	}
	return result, nil
}
