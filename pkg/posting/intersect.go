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
	type postingList struct {
		iterator          OrdinalIterator
		documentFrequency uint64
	}
	postingLists := make([]postingList, 0, len(terms))
	if candidates != nil {
		defer func() { err = errors.Join(err, candidates.Close()) }()
	}
	defer func() {
		for _, list := range postingLists {
			err = errors.Join(err, list.iterator.Close())
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

	// Intersect does not require callers to deduplicate terms. Repeating a term
	// cannot change an AND result, so open and traverse its posting list once.
	seenTerms := make(map[string]bool, len(terms))
	for _, term := range terms {
		if seenTerms[term] {
			continue
		}
		seenTerms[term] = true
		if err := ctx.Err(); err != nil {
			return err
		}
		iterator, documentFrequency, err := opener.OpenOrdinalPostings(ctx, term)
		if err != nil {
			return err
		}
		if iterator == nil {
			return fmt.Errorf("posting: nil iterator")
		}
		postingLists = append(postingLists, postingList{
			iterator:          iterator,
			documentFrequency: documentFrequency,
		})
		if documentFrequency == 0 {
			return iterator.Err()
		}
	}

	slices.SortStableFunc(postingLists, func(a, b postingList) int {
		return cmp.Compare(a.documentFrequency, b.documentFrequency)
	})
	shortestPostingList := postingLists[0]
	var targetOrdinals OrdinalIterator
	var postingListsToSeek []postingList
	targetsComeFromCandidates := false
	switch {
	case candidates == nil:
		// Unrestricted intersection: scan the shortest posting list with Next
		// and align every longer posting list to it with Advance.
		targetOrdinals = shortestPostingList.iterator
		postingListsToSeek = postingLists[1:]
	case candidateCount < shortestPostingList.documentFrequency:
		// Sparse candidate filter (candidateCount < minDF): enumerate only the
		// small candidate set with Next; do not scan any posting list. For each
		// candidate ordinal, Advance seeks every posting list directly to it.
		targetOrdinals = candidates
		postingListsToSeek = postingLists
		targetsComeFromCandidates = true
	default:
		// Dense candidate filter (candidateCount >= minDF): scanning the shortest
		// posting list is no more expensive than scanning all candidates.
		// Candidates are checked only after the posting lists agree on an ordinal.
		targetOrdinals = shortestPostingList.iterator
		postingListsToSeek = postingLists[1:]
	}
	// Next selects the following ordinal when the current target was fully
	// processed. Advance is used only when a posting list is already ahead, so
	// target ordinals known to be absent can be skipped. Every posting list in
	// postingListsToSeek uses Advance because targetOrdinal is already known.
	hasTargetOrdinal := targetOrdinals.Next()

targetLoop:
	for hasTargetOrdinal {
		if err := ctx.Err(); err != nil {
			return err
		}
		targetOrdinal := targetOrdinals.Ord()

		// First apply the required AND: every posting list must contain
		// targetOrdinal. A posting list that is already ahead can skip multiple
		// impossible target ordinals at once.
		for _, list := range postingListsToSeek {
			if !list.iterator.Advance(targetOrdinal) {
				return errors.Join(list.iterator.Err(), ctx.Err())
			}
			if list.iterator.Ord() > targetOrdinal {
				// Advance(targetOrdinal) stopped at a larger ordinal, which proves
				// that this posting list does not contain targetOrdinal. Skip the
				// source of target ordinals to that larger ordinal.
				hasTargetOrdinal = targetOrdinals.Advance(list.iterator.Ord())
				continue targetLoop
			}
		}

		if candidates != nil && !targetsComeFromCandidates {
			// Dense candidates are an additional membership filter. Check them
			// only after the posting-list AND succeeds, so ordinals rejected by
			// another posting list do not cause unnecessary candidate seeks.
			if !candidates.Advance(targetOrdinal) {
				return errors.Join(candidates.Err(), targetOrdinals.Err(), ctx.Err())
			}
			if candidates.Ord() != targetOrdinal {
				// All posting lists contain the ordinal, but candidates do not.
				hasTargetOrdinal = targetOrdinals.Next()
				continue targetLoop
			}
		}

		// Every posting list, and the optional candidate filter, contains the
		// target ordinal. Emit it and continue with the next target.
		if !yield(targetOrdinal) {
			return nil
		}
		hasTargetOrdinal = targetOrdinals.Next()
	}
	var candidateErr error
	if candidates != nil {
		candidateErr = candidates.Err()
	}
	return errors.Join(targetOrdinals.Err(), candidateErr, ctx.Err())
}

func preferSequentialPostingTraversal(candidateCount, documentFrequency uint64) bool {
	// Keeping Advance at equality is faster for the single-list frequency path;
	// unlike multi-list intersection it has no other posting cursors to align.
	return candidateCount > documentFrequency
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
