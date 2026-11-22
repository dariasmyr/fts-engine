package semantic

import (
	"context"
	"slices"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
)

type visitTask struct {
	queryIndex   int
	segmentIndex int
}

func allocateVisitShares(
	ctx context.Context,
	queryCount int,
	segmentCapacities []int,
	budget int,
) ([][]int, error) {
	shares := make([][]int, queryCount)
	for queryIndex := range queryCount {
		shares[queryIndex] = make([]int, len(segmentCapacities))
	}
	tasks := make([]visitTask, 0, queryCount*len(segmentCapacities))
	capacities := make([]int, 0, cap(tasks))
	work := 0
	for segmentRound := range segmentCapacities {
		for queryIndex := range queryCount {
			if err := contextcheck.PeriodicError(ctx, work); err != nil {
				return nil, err
			}
			work++
			segmentIndex := (segmentRound + queryIndex) % len(segmentCapacities)
			tasks = append(tasks, visitTask{
				queryIndex:   queryIndex,
				segmentIndex: segmentIndex,
			})
			capacities = append(capacities, segmentCapacities[segmentIndex])
		}
	}
	taskShares, err := waterFillVisitBudget(ctx, capacities, budget)
	if err != nil {
		return nil, err
	}
	for i, task := range tasks {
		shares[task.queryIndex][task.segmentIndex] = taskShares[i]
	}
	return shares, nil
}

// waterFillVisitBudget distributes a request-wide budget across tasks. Tasks
// are raised evenly up to their capacities; an indivisible remainder follows
// the original task order, which is diagonalized by allocateVisitShares.
func waterFillVisitBudget(ctx context.Context, capacities []int, budget int) ([]int, error) {
	shares := make([]int, len(capacities))
	if len(capacities) == 0 || budget == 0 {
		return shares, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sorted := append([]int(nil), capacities...)
	slices.Sort(sorted)
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	level := 0
	remaining := budget
	remainingTasks := len(sorted)
	for i, capacity := range sorted {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return nil, err
		}
		if capacity > level {
			delta := capacity - level
			if delta > remaining/remainingTasks {
				level += remaining / remainingTasks
				remaining %= remainingTasks
				break
			}
			remaining -= delta * remainingTasks
			level = capacity
		}
		remainingTasks--
		if remainingTasks == 0 {
			remaining = 0
			break
		}
	}
	for i, capacity := range capacities {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return nil, err
		}
		shares[i] = min(level, capacity)
	}
	for i, capacity := range capacities {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return nil, err
		}
		if remaining == 0 {
			break
		}
		if shares[i] < capacity {
			shares[i]++
			remaining--
		}
	}
	return shares, nil
}
