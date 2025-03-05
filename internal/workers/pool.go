package workers

import (
	"context"
	"fmt"
	"sync"
)

type WorkerPool struct {
	workersCount        int
	jobs                chan Job
	results             chan Result
	Done                chan struct{}
	ProcessedTasksCount int
}

func (wp WorkerPool) GenerateFrom(jobsBulk []Job) {
	for i := range jobsBulk {
		wp.jobs <- jobsBulk[i]
	}

	close(wp.jobs)
}

func (wp WorkerPool) Run(ctx context.Context) {
	var wg sync.WaitGroup

	for i := 0; i < wp.workersCount; i++ {
		wg.Add(1)
		go worker(ctx, &wg, wp.jobs, wp.results, &wp.ProcessedTasksCount)
	}

	wg.Wait()
	close(wp.Done)
	close(wp.results)
}

func worker(ctx context.Context, wg *sync.WaitGroup, jobs <-chan Job, results chan<- Result, processedTasksCount *int) {
	defer wg.Done()

	for {
		select {
		case job, ok := <-jobs:
			if !ok {
				return
			}
			result := job.execute(ctx)
			results <- result
			if result.Err == nil {
				*processedTasksCount++
			}
		case <-ctx.Done():
			fmt.Printf("cancelled worker. Error detail: %v/n", ctx.Err())
			results <- Result{
				Err: ctx.Err(),
			}
			processedTasksCount = nil
			return
		}

	}
}

func New(wcount int) WorkerPool {
	return WorkerPool{
		workersCount: wcount,
		jobs:         make(chan Job, wcount),
		results:      make(chan Result, wcount),
		Done:         make(chan struct{}),
	}
}
