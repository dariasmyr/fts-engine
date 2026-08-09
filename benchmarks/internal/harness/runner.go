package harness

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"time"
)

type RunConfig struct {
	Dir         string
	BatchSize   int
	WarmupFrac  float64
	Concurrency int
	Seed        uint64
}

func (c RunConfig) withDefaults() RunConfig {
	if c.BatchSize <= 0 {
		c.BatchSize = 1000
	}
	if c.WarmupFrac < 0 || c.WarmupFrac > 0.5 {
		c.WarmupFrac = 0.10
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.Seed == 0 {
		c.Seed = 0xC0FFEE
	}
	return c
}

type QueryResult struct {
	QueryID string
	Latency time.Duration
	Hits    []SearchHit
}

type HeapStats struct {
	AllocBytes int64
	Objects    int64
}

type Report struct {
	Engine        string
	NumDocs       int
	NumQueries    int
	IndexBuildDur time.Duration
	IndexBytes    int64
	RetainedHeap  *HeapStats
	Latencies     []time.Duration
	Wall          time.Duration
	QueryResults  []QueryResult
}

func Run(ctx context.Context, eng Engine, docs []Document, queries []Query, cfg RunConfig) (*Report, error) {
	prep, err := Prepare(ctx, eng, docs, cfg)
	if err != nil {
		return nil, err
	}
	defer eng.Close()
	return RunQueries(ctx, eng, queries, cfg, prep)
}

func Prepare(ctx context.Context, eng Engine, docs []Document, cfg RunConfig) (_ *Report, err error) {
	cfg = cfg.withDefaults()
	rep := &Report{Engine: eng.Name(), NumDocs: len(docs)}
	before := readHeapSnapshot()

	if err := eng.Open(ctx, cfg.Dir); err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer func() {
		if err != nil {
			_ = eng.Close()
		}
	}()

	buildStart := time.Now()
	for i := 0; i < len(docs); i += cfg.BatchSize {
		end := i + cfg.BatchSize
		if end > len(docs) {
			end = len(docs)
		}
		if err := eng.Index(ctx, docs[i:end]); err != nil {
			return nil, fmt.Errorf("index batch %d: %w", i, err)
		}
	}
	if err := eng.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	rep.IndexBuildDur = time.Since(buildStart)

	if size, err := eng.IndexSizeBytes(); err == nil {
		rep.IndexBytes = size
	}
	after := readHeapSnapshot()
	runtime.KeepAlive(eng)
	runtime.KeepAlive(docs)
	rep.RetainedHeap = &HeapStats{
		AllocBytes: signedDelta(after.allocBytes, before.allocBytes),
		Objects:    signedDelta(after.objects, before.objects),
	}

	return rep, nil
}

func RunQueries(ctx context.Context, eng Engine, queries []Query, cfg RunConfig, base *Report) (*Report, error) {
	cfg = cfg.withDefaults()
	rep := &Report{Engine: eng.Name(), NumQueries: len(queries)}
	if base != nil {
		rep.NumDocs = base.NumDocs
		rep.IndexBuildDur = base.IndexBuildDur
		rep.IndexBytes = base.IndexBytes
		rep.RetainedHeap = base.RetainedHeap
	}

	shuffled := append([]Query(nil), queries...)
	rng := rand.New(rand.NewSource(int64(cfg.Seed)))
	rng.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	warmupN := int(math.Floor(float64(len(shuffled)) * cfg.WarmupFrac))
	for i := 0; i < warmupN; i++ {
		if _, err := eng.Search(ctx, shuffled[i]); err != nil {
			return nil, fmt.Errorf("warmup search %q: %w", shuffled[i].ID, err)
		}
	}
	measure := shuffled[warmupN:]

	rep.Latencies = make([]time.Duration, len(measure))
	rep.QueryResults = make([]QueryResult, len(measure))

	measureStart := time.Now()
	if cfg.Concurrency == 1 {
		for i, q := range measure {
			t0 := time.Now()
			hits, err := eng.Search(ctx, q)
			if err != nil {
				return nil, fmt.Errorf("search %q: %w", q.ID, err)
			}
			dt := time.Since(t0)
			rep.Latencies[i] = dt
			rep.QueryResults[i] = QueryResult{QueryID: q.ID, Latency: dt, Hits: hits}
		}
	} else {
		var wg sync.WaitGroup
		jobs := make(chan int)
		errCh := make(chan error, len(measure))
		for worker := 0; worker < cfg.Concurrency; worker++ {
			wg.Go(
				func() {
					for i := range jobs {
						q := measure[i]
						t0 := time.Now()
						hits, err := eng.Search(ctx, q)
						dt := time.Since(t0)
						if err != nil {
							errCh <- fmt.Errorf("search %q: %w", q.ID, err)
							continue
						}
						rep.Latencies[i] = dt
						rep.QueryResults[i] = QueryResult{QueryID: q.ID, Latency: dt, Hits: hits}
					}
				},
			)
		}
		for i := range measure {
			jobs <- i
		}
		close(jobs)
		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil {
				return nil, err
			}
		}
	}
	rep.Wall = time.Since(measureStart)

	return rep, nil
}

type heapSnapshot struct {
	allocBytes uint64
	objects    uint64
}

func readHeapSnapshot() heapSnapshot {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return heapSnapshot{allocBytes: stats.HeapAlloc, objects: stats.HeapObjects}
}

func signedDelta(after, before uint64) int64 {
	if after >= before {
		return int64(after - before)
	}
	return -int64(before - after)
}

func Percentiles(lats []time.Duration, ps ...float64) []time.Duration {
	if len(lats) == 0 {
		return make([]time.Duration, len(ps))
	}
	cp := append([]time.Duration(nil), lats...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	out := make([]time.Duration, len(ps))
	for i, p := range ps {
		switch {
		case p < 0:
			p = 0
		case p > 1:
			p = 1
		}
		idx := int(float64(len(cp)-1) * p)
		out[i] = cp[idx]
	}
	return out
}
