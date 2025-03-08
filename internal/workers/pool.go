package workers

import (
	"context"
	"fmt"
	"sync"
)

type WorkerPool struct {
	workersCount  int
	jobs          chan Job
	results       chan Result
	Done          chan struct{}
	activeWorkers chan int
}

func (wp WorkerPool) GenerateFrom(jobsBulk []Job) {
	for i := range jobsBulk {
		wp.jobs <- jobsBulk[i]
	}

	close(wp.jobs)
}

func (wp WorkerPool) AddJob(job *Job) {
	wp.jobs <- *job
}

func (wp WorkerPool) ActiveWorkersCount() int {
	return len(wp.activeWorkers)
}

func (wp WorkerPool) JobChannelCount() int {
	return len(wp.jobs)
}

func (wp WorkerPool) Run(ctx context.Context) {
	var wg sync.WaitGroup

	for i := 0; i < wp.workersCount; i++ {
		wg.Add(1)
		go worker(ctx, &wg, wp.jobs, wp.results, wp.activeWorkers)
	}

	wg.Wait()
	close(wp.Done)
	close(wp.results)
}

func worker(ctx context.Context, wg *sync.WaitGroup, jobs <-chan Job, results chan<- Result, activeWorkers chan int) {
	defer wg.Done()

	activeWorkers <- 1

	for {
		select {
		case job, ok := <-jobs:
			if !ok {
				<-activeWorkers
				return
			}
			result := job.execute(ctx)
			results <- result
		case <-ctx.Done():
			fmt.Printf("cancelled worker. Error detail: %v/n", ctx.Err())
			results <- Result{
				Err: ctx.Err(),
			}
			<-activeWorkers
			return
		}

	}
}

func New(wcount int, jobcount int) WorkerPool {
	return WorkerPool{
		workersCount:  wcount,
		jobs:          make(chan Job, jobcount),
		results:       make(chan Result, jobcount),
		activeWorkers: make(chan int, wcount),
		Done:          make(chan struct{}),
	}
}
