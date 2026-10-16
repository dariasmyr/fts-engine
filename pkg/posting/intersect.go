package posting

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
)

// Intersect streams the AND of exact, already-analyzed terms in ascending
// ordinal order. Nil candidates mean unrestricted; a non-nil empty slice means
// no matches. Candidates must be strictly increasing.
func Intersect(ctx context.Context, opener OrdinalPostingOpener, terms []string, candidates []Ord, yield func(Ord) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if opener == nil || yield == nil {
		return fmt.Errorf("posting: nil opener or callback")
	}
	if err := validateCandidates(ctx, candidates); err != nil {
		return err
	}
	var cursor OrdinalIterator
	if candidates != nil {
		cursor = &sliceIterator{ords: candidates, pos: -1}
	}
	return IntersectWithIterator(ctx, opener, terms, cursor, uint64(len(candidates)), yield)
}

// IntersectWithIterator is Intersect with a streaming candidate set. A nil
// iterator means unrestricted. candidateCount must be the exact number of
// ordinals produced by candidates. This function owns and closes candidates.
func IntersectWithIterator(
	ctx context.Context,
	opener OrdinalPostingOpener,
	terms []string,
	candidates OrdinalIterator,
	candidateCount uint64,
	yield func(Ord) bool,
) (err error) {
	type input struct {
		cursor OrdinalIterator
		count  uint64
	}
	inputs := make([]input, 0, len(terms)+1)
	if candidates != nil {
		inputs = append(inputs, input{cursor: candidates, count: candidateCount})
	}
	defer func() {
		for _, in := range inputs {
			err = errors.Join(err, in.cursor.Close())
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if opener == nil || yield == nil {
		return fmt.Errorf("posting: nil opener or callback")
	}
	if len(terms) == 0 {
		return nil
	}
	if candidates != nil && candidateCount == 0 {
		return candidates.Err()
	}

	seen := make(map[string]bool, len(terms))
	for _, term := range terms {
		if seen[term] {
			continue
		}
		seen[term] = true
		if err := ctx.Err(); err != nil {
			return err
		}
		cursor, count, err := opener.OpenOrdinalPostings(ctx, term)
		if err != nil {
			return err
		}
		if cursor == nil {
			return fmt.Errorf("posting: nil iterator")
		}
		inputs = append(inputs, input{cursor: cursor, count: count})
		if count == 0 {
			return cursor.Err()
		}
	}

	slices.SortStableFunc(inputs, func(a, b input) int { return cmp.Compare(a.count, b.count) })
	driver := inputs[0].cursor
	for valid := driver.Next(); valid; {
		if err := ctx.Err(); err != nil {
			return err
		}
		target := driver.Ord()
		matched := true
		for _, in := range inputs[1:] {
			if !in.cursor.Advance(target) {
				return errors.Join(in.cursor.Err(), ctx.Err())
			}
			if in.cursor.Ord() > target {
				valid = driver.Advance(in.cursor.Ord())
				matched = false
				break
			}
		}
		if matched {
			if !yield(target) {
				return nil
			}
			valid = driver.Next()
		}
	}
	return errors.Join(driver.Err(), ctx.Err())
}

type sliceIterator struct {
	ords []Ord
	pos  int
}

func (i *sliceIterator) Next() bool {
	if i.pos < len(i.ords) {
		i.pos++
	}
	return i.pos < len(i.ords)
}

func (i *sliceIterator) Advance(target Ord) bool {
	if i.pos >= len(i.ords) {
		return false
	}
	if i.pos >= 0 && i.ords[i.pos] >= target {
		return true
	}
	start := i.pos + 1
	i.pos = start + sort.Search(len(i.ords)-start, func(n int) bool { return i.ords[start+n] >= target })
	return i.pos < len(i.ords)
}

func (i *sliceIterator) Ord() Ord     { return i.ords[i.pos] }
func (i *sliceIterator) Err() error   { return nil }
func (i *sliceIterator) Close() error { i.pos = len(i.ords); return nil }

func validateCandidates(ctx context.Context, candidates []Ord) error {
	for i := 1; i < len(candidates); i++ {
		if i%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if candidates[i] <= candidates[i-1] {
			return fmt.Errorf("posting: candidates must be strictly increasing")
		}
	}
	return nil
}
