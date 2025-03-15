package workers

import (
	"context"
	"fmt"
	"os"
	"sync"
)

type WorkerPool struct {
	workersCount int
	jobs         chan Job
	Done         chan struct{}
	logFile      *os.File
	logMutex     sync.Mutex
}

type JobError struct {
	JobDescription JobDescriptor `json:"job_description"`
	Error          string        `json:"error"`
}

func (wp *WorkerPool) AddJob(job Job) {
	wp.jobs <- job
}

func (wp *WorkerPool) Run(ctx context.Context) {
	var wg sync.WaitGroup

	for i := 0; i < wp.workersCount; i++ {
		wg.Add(1)
		go worker(ctx, &wg, wp)
	}

	wg.Wait()
	close(wp.jobs)
	close(wp.Done)
}

func worker(ctx context.Context, wg *sync.WaitGroup, wp *WorkerPool) {
	defer wg.Done()

	for {
		select {
		case job, ok := <-wp.jobs:
			if !ok {
				return
			}
			result := job.execute(ctx)
			if result.Err != nil {
				fmt.Printf("Job %+v failed: %v\n", job.Description, result.Err)
			}
			fmt.Printf("Job %+v completed\n", job.Description)
		case <-ctx.Done():
			fmt.Printf("Worker cancelled: %v\n", ctx.Err())
			return
		}
	}
}

func New(numWorkers int) *WorkerPool {
	return &WorkerPool{
		workersCount: numWorkers,
		jobs:         make(chan Job),
		Done:         make(chan struct{}),
	}
}
