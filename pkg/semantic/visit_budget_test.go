package semantic

import (
	"context"
	"errors"
	"slices"
	"testing"
)

func TestWaterFillVisitBudget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		capacities []int
		budget     int
		want       []int
	}{
		{
			name: "no tasks",
		},
		{
			name:       "zero budget",
			capacities: []int{5, 5, 5},
			want:       []int{0, 0, 0},
		},
		{
			name:       "remainder follows task order",
			capacities: []int{5, 5, 5},
			budget:     4,
			want:       []int{2, 1, 1},
		},
		{
			name:       "small task capacity is redistributed",
			capacities: []int{1, 3, 5},
			budget:     6,
			want:       []int{1, 3, 2},
		},
		{
			name:       "zero capacity is skipped",
			capacities: []int{0, 2, 2},
			budget:     3,
			want:       []int{0, 2, 1},
		},
		{
			name:       "budget above total capacity saturates tasks",
			capacities: []int{1, 3, 5},
			budget:     20,
			want:       []int{1, 3, 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := waterFillVisitBudget(context.Background(), tt.capacities, tt.budget)
			if err != nil {
				t.Fatalf("waterFillVisitBudget() error = %v", err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("waterFillVisitBudget() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWaterFillVisitBudgetChecksContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := waterFillVisitBudget(ctx, []int{1, 2, 3}, 3)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waterFillVisitBudget() error = %v, want %v", err, context.Canceled)
	}
}

func TestAllocateVisitSharesUsesDiagonalRemainderOrder(t *testing.T) {
	t.Parallel()

	got, err := allocateVisitShares(context.Background(), 2, []int{5, 5, 5}, 4)
	if err != nil {
		t.Fatalf("allocateVisitShares() error = %v", err)
	}
	want := [][]int{
		{1, 1, 0},
		{0, 1, 1},
	}
	if len(got) != len(want) {
		t.Fatalf("allocateVisitShares() rows = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !slices.Equal(got[i], want[i]) {
			t.Fatalf("allocateVisitShares()[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}
