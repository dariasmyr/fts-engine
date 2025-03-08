package frequency

import (
	"time"
)

type Frequency struct {
	Interval time.Duration
	count    int
	total    int
	LastTime time.Time
	Stats    *Stats
}

func New(interval time.Duration) *Frequency {
	return &Frequency{
		Interval: interval,
		LastTime: time.Now(),
		Stats:    &Stats{},
	}
}

type Stats struct {
	Total   int
	Count   int
	Average float64
}

func (f *Frequency) Add(count int) {
	f.count += count
	f.total += count
}

func (f *Frequency) PrintFreq() {
	now := time.Now()
	elapsed := now.Sub(f.LastTime)
	if elapsed >= f.Interval {
		average := float64(f.count) / elapsed.Seconds()
		f.Stats.Total = f.total
		f.Stats.Count = f.count
		f.Stats.Average = average
		f.LastTime = now
		f.count = 0
	}
}
