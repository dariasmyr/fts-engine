package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/dariasmyr/fts-engine/v2/pkg/index/flat"
	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
	"github.com/dariasmyr/fts-engine/v2/pkg/presence"
	"github.com/dariasmyr/fts-engine/v2/pkg/textproc"
)

func main() {
	if err := run(os.Stdout); err != nil {
		panic(err)
	}
}

func run(out io.Writer) error {
	ctx := context.Background()
	// The embedding store owns numeric IDs, analyzer version and ordinal mapping.
	ids := []uint64{100000001, 100000008, 100000042}
	bodies := []string{"checkout timeout io.EOF", "billing timeout", "checkout timeout"}
	// Use the same pipeline for documents and queries. Amber currently uses this
	// multilingual pipeline; ObservabilityPipeline has different semantics.
	pipe := textproc.DefaultMultilingualPipeline()
	builder := flat.New()
	for ord, body := range bodies {
		for _, key := range pipe.Process(body) {
			// Plain Insert avoids positional storage during build.
			if err := builder.Insert(key, posting.Ord(ord)); err != nil {
				return err
			}
		}
	}
	// Stream to a temporary file: no second complete encoded index on the heap.
	file, err := os.CreateTemp("", "fts-presence-*.fpr")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if err := presence.WriteFromSource(ctx, file, builder); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	// A store would publish this file atomically together with the ID table and
	// analyzer metadata, then keep it open for the lifetime of its queries.
	reader, err := presence.OpenFile(file.Name())
	if err != nil {
		return err
	}
	defer reader.Close()
	// For example, this bitmap may come from an external service=checkout index.
	// Candidate IDs are translated into this FPR1 generation's ordinal space.
	// IDs absent from the generation are ignored.
	candidateWords, candidateCount := candidateBitmap(
		ids,
		[]uint64{100000001, 100000042, 100000099},
	)
	candidates := posting.NewBitmapIterator(candidateWords)
	var outputErr error
	// IntersectWithIterator owns and closes candidates. candidateCount must be
	// the exact number of set ordinals in candidateWords.
	err = posting.IntersectWithIterator(ctx, reader, pipe.Process("timeout"), candidates, candidateCount, func(ord posting.Ord) bool {
		if uint64(ord) >= uint64(len(ids)) {
			outputErr = fmt.Errorf("ordinal %d is outside the ID table", ord)
			return false
		}
		_, outputErr = fmt.Fprintln(out, ids[ord])
		if outputErr != nil {
			return false
		}
		return true // Keep all candidates for subsequent predicates/time sorting.
	})
	if err != nil {
		return err
	}
	return outputErr
}

func candidateBitmap(ids, candidates []uint64) ([]uint64, uint64) {
	if candidates == nil {
		return nil, 0
	}
	words := make([]uint64, (len(ids)+63)/64)
	var count uint64
	for _, id := range candidates {
		if i, ok := slices.BinarySearch(ids, id); ok {
			word, bit := i/64, uint(i%64)
			mask := uint64(1) << bit
			if words[word]&mask == 0 {
				words[word] |= mask
				count++
			}
		}
	}
	return words, count
}
