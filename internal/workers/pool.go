package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
)

type WorkerPool struct {
	workersCount  int
	jobs          chan Job
	Done          chan struct{}
	activeWorkers int32
	logFile       *os.File
	logMutex      sync.Mutex
}

type JobError struct {
	JobDescription JobDescriptor `json:"job_description"`
	Error          string        `json:"error"`
}

func (wp *WorkerPool) AddJob(job *Job) {
	wp.jobs <- *job
}

func (wp *WorkerPool) ActiveWorkersCount() int32 {
	return atomic.LoadInt32(&wp.activeWorkers)
}

func (wp *WorkerPool) MemoryUsage() uint64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return memStats.Alloc
}

func (wp *WorkerPool) Run(ctx context.Context) {
	var wg sync.WaitGroup

	var err error
	wp.logFile, err = os.Create("worker_errors.json")
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

func (wp *WorkerPool) CloseLogFile() error {
	err := wp.logFile.Close()
	if err != nil {
		return fmt.Errorf("Error closing log file: %v", err)
	}
	return nil
}

func worker(ctx context.Context, wg *sync.WaitGroup, wp *WorkerPool) {
	defer wg.Done()

	atomic.AddInt32(&wp.activeWorkers, 1)
	defer atomic.AddInt32(&wp.activeWorkers, -1)

	for {
		select {
		case job, ok := <-wp.jobs:
			if !ok {
				return
			}
			result := job.execute(ctx)
			if result.Err != nil {
				jobErr := JobError{
					JobDescription: job.Description,
					Error:          result.Err.Error(),
				}
				wp.logMutex.Lock()
				encoder := json.NewEncoder(wp.logFile)
				if err := encoder.Encode(jobErr); err != nil {
					fmt.Printf("Failed to write JSON log: %v\n", err)
				}
				wp.logMutex.Unlock()
			}
		case <-ctx.Done():
			fmt.Printf("Worker cancelled: %v\n", ctx.Err())
			return
		}
	}
}

func (wp *WorkerPool) JobChannelCount() int {
	return len(wp.jobs)
}

func New(numWorkers int) WorkerPool {
	return WorkerPool{
		workersCount: numWorkers,
		jobs:         make(chan Job),
		Done:         make(chan struct{}),
	}
}
