package metrics

import (
	"log/slog"
	"sync"
	"time"
)

type Metrics struct {
	mu                 sync.Mutex
	totalJobs          int
	successfulJobs     int
	failedJobs         int
	totalExecutionTime time.Duration
	executionCount     int
}

func (m *Metrics) RecordSuccess(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalJobs++
	m.successfulJobs++
	m.totalExecutionTime += duration
	m.executionCount++
}

func (m *Metrics) RecordFailure(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalJobs++
	m.failedJobs++
	m.totalExecutionTime += duration
	m.executionCount++
}

func (m *Metrics) PrintMetrics(log *slog.Logger) {
	m.mu.Lock()
	defer m.mu.Unlock()

	avgExecTime := time.Duration(0)
	if m.executionCount > 0 {
		avgExecTime = m.totalExecutionTime / time.Duration(m.executionCount)
	}

	log.Info("Metrics",
		"Total Jobs", m.totalJobs,
		"Successful Jobs", m.successfulJobs,
		"Failed Jobs", m.failedJobs,
		"Avg Execution Time", avgExecTime,
	)
}
