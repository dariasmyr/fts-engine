package metrics

import (
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

type Stats struct {
	TotalJobs      int
	SuccessfulJobs int
	FailedJobs     int
	AvgExecTime    time.Duration
}

func New() *Metrics {
	return &Metrics{}
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

func (m *Metrics) PrintMetrics() *Stats {
	m.mu.Lock()
	defer m.mu.Unlock()

	avgExecTime := time.Duration(0)
	if m.executionCount > 0 {
		avgExecTime = m.totalExecutionTime / time.Duration(m.executionCount)
	}

	return &Stats{
		TotalJobs:      m.totalJobs,
		SuccessfulJobs: m.successfulJobs,
		FailedJobs:     m.failedJobs,
		AvgExecTime:    avgExecTime,
	}
}
