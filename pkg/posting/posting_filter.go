package posting

import (
	"context"
	"errors"
	"fmt"
)

// FilterPostings streams one exact posting list, optionally restricted by
// strictly increasing candidate ordinals. It returns the unfiltered document
// frequency. Nil candidates mean unrestricted; a non-nil empty slice emits no
// postings.
func FilterPostings(
	ctx context.Context,
	opener FrequencyPostingOpener,
	term string,
	candidates []Ord,
	yield func(Posting) bool,
) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := validateCandidates(ctx, candidates); err != nil {
		return 0, err
	}
	var cursor OrdinalIterator
	if candidates != nil {
		cursor = &sliceIterator{ords: candidates, pos: -1}
	}
	return FilterPostingsWithIterator(ctx, opener, term, cursor, uint64(len(candidates)), yield)
}

// FilterPostingsWithIterator is FilterPostings with a streaming candidate set.
// candidateCount must be the exact number of ordinals produced by candidates.
// This function owns and closes candidates.
func FilterPostingsWithIterator(
	ctx context.Context,
	opener FrequencyPostingOpener,
	term string,
	candidates OrdinalIterator,
	candidateCount uint64,
	yield func(Posting) bool,
) (documentFrequency uint64, err error) {
	closeCandidates := candidates != nil
	defer func() {
		if closeCandidates {
			err = errors.Join(err, candidates.Close())
		}
	}()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if opener == nil || yield == nil {
		return 0, fmt.Errorf("posting: nil posting opener or callback")
	}
	postingCursor, count, err := opener.OpenFrequencyPostings(ctx, term)
	if err != nil {
		return 0, err
	}
	if postingCursor == nil {
		return 0, fmt.Errorf("posting: nil posting iterator")
	}
	closeCandidates = false
	return count, FilterPostingIterator(ctx, postingCursor, count, candidates, candidateCount, yield)
}

// FilterPostingIterator intersects an already-open frequency iterator with an
// optional candidate iterator. The counts must be exact. It owns and closes
// both iterators, allowing callers to use postingCount for result sizing before
// traversal starts.
func FilterPostingIterator(
	ctx context.Context,
	postingCursor FrequencyIterator,
	postingCount uint64,
	candidates OrdinalIterator,
	candidateCount uint64,
	yield func(Posting) bool,
) (err error) {
	if candidates != nil {
		defer func() { err = errors.Join(err, candidates.Close()) }()
	}
	if postingCursor != nil {
		defer func() { err = errors.Join(err, postingCursor.Close()) }()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if postingCursor == nil || yield == nil {
		return fmt.Errorf("posting: nil posting iterator or callback")
	}
	if postingCount == 0 {
		return postingCursor.Err()
	}
	if candidates != nil && candidateCount == 0 {
		return candidates.Err()
	}

	if candidates == nil {
		return yieldAllPostings(ctx, postingCursor, yield)
	}

	// Drive the intersection with the smaller input. The other iterator uses
	// Advance to skip directly to the current ordinal.
	if candidateCount >= postingCount {
		return filterByIteratingPostings(ctx, postingCursor, candidates, yield)
	}
	return filterByIteratingCandidates(ctx, postingCursor, candidates, yield)
}

func yieldAllPostings(
	ctx context.Context,
	postings FrequencyIterator,
	yield func(Posting) bool,
) error {
	for postings.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !yieldCurrentPosting(postings, yield) {
			return nil
		}
	}
	return errors.Join(postings.Err(), ctx.Err())
}

func filterByIteratingPostings(
	ctx context.Context,
	postings FrequencyIterator,
	candidates OrdinalIterator,
	yield func(Posting) bool,
) error {
	for postings.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !candidates.Advance(postings.Ord()) {
			return errors.Join(candidates.Err(), postings.Err(), ctx.Err())
		}
		if candidates.Ord() == postings.Ord() && !yieldCurrentPosting(postings, yield) {
			return nil
		}
	}
	return errors.Join(postings.Err(), candidates.Err(), ctx.Err())
}

func filterByIteratingCandidates(
	ctx context.Context,
	postings FrequencyIterator,
	candidates OrdinalIterator,
	yield func(Posting) bool,
) error {
	for candidates.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		target := candidates.Ord()
		if !postings.Advance(target) {
			return errors.Join(postings.Err(), candidates.Err(), ctx.Err())
		}
		if postings.Ord() == target && !yieldCurrentPosting(postings, yield) {
			return nil
		}
	}
	return errors.Join(candidates.Err(), ctx.Err())
}

func yieldCurrentPosting(postings FrequencyIterator, yield func(Posting) bool) bool {
	ord := postings.Ord()
	return yield(Posting{
		Ord:   ord,
		Count: postings.Frequency(),
	})
}
