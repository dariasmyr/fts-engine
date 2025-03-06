package frequency

import (
	"log/slog"
	"time"
)

type Frequency struct {
	Interval time.Duration
	count    int
	total    int
	LastTime time.Time
}

func (f *Frequency) Add(count int) {
	f.count += count
	f.total += count
}

func (f *Frequency) Check(log *slog.Logger) {
	now := time.Now()
	elapsed := now.Sub(f.LastTime)
	if elapsed >= f.Interval {
		average := float64(f.total) / elapsed.Seconds()
		log.Info("Event Rate", "count", f.count, "average", average)
		f.LastTime = now
	}
}
