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
	result       chan Result
	Done         chan struct{}
	logFile      *os.File
	logMutex     sync.Mutex
}

type JobError struct {
	JobDescription JobDescriptor `json:"job_description"`
	Error          string        `json:"error"`
}

func (wp *WorkerPool) AddJob(job *Job) {
	wp.jobs <- *job
}

func (wp *WorkerPool) Run(ctx context.Context) {
	var wg sync.WaitGroup

	var err error
	if err != nil {
		fmt.Printf("Error opening log file: %v\n", err)
		return
	}

	for i := 0; i < wp.workersCount; i++ {
		wg.Add(1)
		go worker(ctx, &wg, wp)
	}

	wg.Wait()
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
				wp.result <- result
			}
			fmt.Printf("Job %s completed\n", job.Description)
		case <-ctx.Done():
			fmt.Printf("Worker cancelled: %v\n", ctx.Err())
			return
		}
	}
}

func New(numWorkers int, numJobs int) *WorkerPool {
	return &WorkerPool{
		workersCount: numWorkers,
		jobs:         make(chan Job, numJobs),
		result:       make(chan Result, numJobs),
		Done:         make(chan struct{}),
	}
}
