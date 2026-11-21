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
	// verified is build-only state: this link passed diversification together
	// with every other verified link currently owned by the same node and level.
	verified bool
}

type searchExecution struct {
	ctx        context.Context
	index      *Index
	query      vector.Prepared
	filter     vector.ResultFilter
	visitLimit int
	workspace  *searchWorkspace
	stats      SearchStats
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

func search(ctx context.Context, reader *Index, query []float32, prepared *vector.Prepared, k int, config QueryConfig) (SearchResult, error) {
	if ctx == nil {
		return SearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	if reader == nil {
		return SearchResult{}, errInvalidGraph
	}
	spec := reader.topology.spec
	vectorCount := reader.Len()
	request, err := resolveSearchRequest(reader.config.Search, vectorCount, k, config)
	if err != nil {
		return SearchResult{}, err
	}
	workspace := reader.workspaces.acquire()
	defer reader.workspaces.release(workspace)
	var preparedQuery vector.Prepared
	if prepared == nil {
		workspace.prepareQuery(spec.Dimensions())
		preparedQuery, err = spec.PrepareInto(workspace.preparedQuery, query)
	} else {
		err = spec.ValidatePrepared(*prepared)
		preparedQuery = *prepared
	}
	if err != nil {
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
	workspace.resetSearch(vectorCount, 0, request.visitLimit, request.appliedEfSearch)
	entry, maxLevel, ok := reader.entryPoint()
	if !ok || maxLevel < 0 || maxLevel > maxLevelLimit || uint64(entry) >= uint64(vectorCount) {
		return SearchResult{}, errInvalidGraph
	}
	execution := searchExecution{
		ctx: ctx, index: reader, query: preparedQuery, filter: request.filter,
		visitLimit: request.visitLimit,
		workspace:  workspace,
		stats:      stats,
	}
	entryCandidate, err := execution.candidateForNode(entry)
	if err != nil {
		return finishSearch(execution, resultHeap{}, request.k, err)
	}
	// Sparse upper levels choose one local minimum; level 0 then widens into a beam.
	entryCandidate, err = greedySearch(&execution, entryCandidate, maxLevel)
	if err != nil {
		return finishSearch(execution, execution.acceptedResults(min(request.appliedEfSearch, request.allowedCount)), request.k, err)
	}
	workspace.stopTrackingScored()
	results, err := levelSearch(&execution, entryCandidate, request.appliedEfSearch, request.allowedCount)
	return finishSearch(execution, results, request.k, err)
}

func greedySearch(execution *searchExecution, currentCandidate candidate, maxLevel int) (candidate, error) {
	for level := maxLevel; level > 0; level-- {
		for {
			if err := execution.ctx.Err(); err != nil {
				return candidate{}, err
			}
			execution.stats.ExpandedNodes++
			best := currentCandidate
			neighbors, ok := execution.index.neighborView(currentCandidate.node, level)
			if !ok {
				return candidate{}, errInvalidGraph
			}
			for start := 0; start < len(neighbors); start += 4 {
				count := min(4, len(neighbors)-start)
				var nodes [4]nodeOrdinal
				for i := range count {
					if err := periodicContextError(execution.ctx, start+i); err != nil {
						return candidate{}, err
					}
					nodes[i] = neighbors[start+i]
				}
				candidateBatch, _, err := execution.candidatesForNodes(nodes, count)
				if err != nil {
					return candidate{}, err
				}
				for i := range count {
					candidate := candidateBatch[i]
					if candidate.distance < currentCandidate.distance && candidateCloser(candidate, best) {
						best = candidate
					}
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
	execution *searchExecution,
	entry candidate,
	efSearch, allowedCount int,
) (results resultHeap, err error) {
	resultCapacity := min(efSearch, allowedCount)

	results = newResultHeapWithBuffer(
		resultCapacity,
		execution.workspace.results,
	)

	candidates := candidateHeap{
		items: execution.workspace.frontier,
	}

	defer func() {
		execution.workspace.results = results.items[:0]
		execution.workspace.frontier = candidates.items[:0]
	}()

	execution.workspace.markSeen(entry.node)
	candidates.Push(entry)

	if entry.accepted {
		results.Add(entry)
	}

	for candidates.Len() > 0 {
		if err := execution.ctx.Err(); err != nil {
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

		execution.stats.ExpandedNodes++

		neighbors, ok := execution.index.neighborView(
			candidate.node,
			0,
		)
		if !ok {
			return results, errInvalidGraph
		}

		var neighborBatch [4]nodeOrdinal
		batchCount := 0
		for i, neighbor := range neighbors {
			if err := periodicContextError(execution.ctx, i); err != nil {
				return results, err
			}

			if uint64(neighbor) >= uint64(execution.index.Len()) {
				return results, errInvalidGraph
			}

			if !execution.workspace.markSeen(neighbor) {
				continue
			}
			neighborBatch[batchCount] = neighbor
			batchCount++
			if batchCount == len(neighborBatch) {
				discovered, validCount, candidateErr := execution.candidatesForNodes(neighborBatch, batchCount)
				for j := range validCount {
					addDiscoveredCandidate(discovered[j], efSearch, allowedCount, &results, &candidates)
				}
				if candidateErr != nil {
					return results, candidateErr
				}
				batchCount = 0
			}
		}
		if batchCount > 0 {
			discovered, validCount, candidateErr := execution.candidatesForNodes(neighborBatch, batchCount)
			for i := range validCount {
				addDiscoveredCandidate(discovered[i], efSearch, allowedCount, &results, &candidates)
			}
			if candidateErr != nil {
				return results, candidateErr
			}
		}
	}

	return results, nil
}

func addDiscoveredCandidate(
	discovered candidate,
	efSearch, allowedCount int,
	results *resultHeap,
	candidates *candidateHeap,
) {
	if discovered.accepted {
		results.Add(discovered)
	}
	// All living (allowed by the result filter) vectors have been found, so no
	// more candidates need to be added to the frontier.
	if results.Len() == allowedCount {
		return
	}
	worst, full := results.Worst()
	if !full || results.Len() < efSearch || discovered.distance <= worst.distance {
		candidates.Push(discovered)
	}
}

func (e *searchExecution) candidateForNode(node nodeOrdinal) (candidate, error) {
	if uint64(node) >= uint64(e.index.Len()) {
		return candidate{}, errInvalidGraph
	}
	if candidate, exists := e.workspace.candidate(node); exists {
		return candidate, nil
	}
	// VisitedNodes and the visit budget count unique query-to-node distances across
	// all levels. The cache prevents duplicate distance work through converging paths.
	if e.stats.VisitedNodes >= e.visitLimit {
		return candidate{}, errVisitLimit
	}
	ordinal := vector.Ordinal(node)
	if err := e.ctx.Err(); err != nil {
		return candidate{}, err
	}
	distance, err := e.index.matrix.DistancePrepared(e.query, ordinal)
	if err != nil {
		return candidate{}, fmt.Errorf("%w: calculate distance to vector row %d: %v", errInvalidGraph, ordinal, err)
	}
	if math.IsNaN(distance) || math.IsInf(distance, 0) {
		return candidate{}, errInvalidGraph
	}
	accepted := e.filter == nil || e.filter.Allows(ordinal)
	candidate := candidate{node: node, distance: distance, accepted: accepted}
	e.workspace.storeCandidate(candidate)
	e.stats.VisitedNodes++
	e.stats.DistanceComputations++
	if !accepted {
		e.stats.RejectedNodes++
	}
	return candidate, nil
}

func (e *searchExecution) candidatesForNodes(nodes [4]nodeOrdinal, count int) ([4]candidate, int, error) {
	if count < 1 || count > len(nodes) {
		return [4]candidate{}, 0, errInvalidGraph
	}

	var ordinals [4]vector.Ordinal
	missingCount := 0
	for i := range count {
		node := nodes[i]
		if uint64(node) >= uint64(e.index.Len()) {
			return [4]candidate{}, 0, errInvalidGraph
		}
		if _, exists := e.workspace.candidate(node); exists {
			continue
		}
		ordinal := vector.Ordinal(node)
		duplicate := false
		for j := range missingCount {
			if ordinals[j] == ordinal {
				duplicate = true
				break
			}
		}
		if !duplicate {
			ordinals[missingCount] = ordinal
			missingCount++
		}
	}

	// Preserve the scalar visit-limit behavior near the end of the budget. It
	// may produce a valid prefix before returning errVisitLimit.
	if missingCount > e.visitLimit-e.stats.VisitedNodes {
		var result [4]candidate
		for i := range count {
			candidate, err := e.candidateForNode(nodes[i])
			if err != nil {
				return result, i, err
			}
			result[i] = candidate
		}
		return result, count, nil
	}
	if missingCount > 0 {
		if err := e.ctx.Err(); err != nil {
			return [4]candidate{}, 0, err
		}
		distances, err := e.index.matrix.DistancePreparedBatch(e.query, ordinals, missingCount)
		if err != nil {
			return [4]candidate{}, 0, fmt.Errorf("%w: calculate distances to vector batch: %v", errInvalidGraph, err)
		}
		for i := range missingCount {
			distance := distances[i]
			if math.IsNaN(distance) || math.IsInf(distance, 0) {
				return [4]candidate{}, 0, errInvalidGraph
			}
			ordinal := ordinals[i]
			accepted := e.filter == nil || e.filter.Allows(ordinal)
			candidate := candidate{
				node:     nodeOrdinal(ordinal),
				distance: distance,
				accepted: accepted,
			}
			e.workspace.storeCandidate(candidate)
			e.stats.VisitedNodes++
			e.stats.DistanceComputations++
			if !accepted {
				e.stats.RejectedNodes++
			}
		}
	}

	var result [4]candidate
	for i := range count {
		result[i], _ = e.workspace.candidate(nodes[i])
	}
	return result, count, nil
}

func (e *searchExecution) acceptedResults(capacity int) resultHeap {
	results := newResultHeapWithBuffer(capacity, e.workspace.results)
	for _, node := range e.workspace.scoredNodes {
		candidate, _ := e.workspace.candidate(node)
		if candidate.accepted {
			results.Add(candidate)
		}
	}
	e.workspace.results = results.items[:0]
	return results
}

func periodicContextError(ctx context.Context, iteration int) error {
	if iteration&63 != 0 {
		return nil
	}
	return ctx.Err()
}

func finishSearch(execution searchExecution, results resultHeap, k int, err error) (SearchResult, error) {
	if err != nil && !errors.Is(err, errVisitLimit) {
		return SearchResult{}, err
	}
	if contextErr := execution.ctx.Err(); contextErr != nil {
		return SearchResult{}, contextErr
	}
	result := SearchResult{Hits: results.Hits(k), Stats: execution.stats}
	if errors.Is(err, errVisitLimit) {
		result.Incomplete = true
		result.Stats.Termination = TerminationVisitLimit
	}
	return result, nil
}
