package ftsstats

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

const defaultRecentLimit = 64

type SearchStats struct {
	mu sync.Mutex

	totalSearches int
	errorsTotal   int
	zeroResults   int
	byStrategy    map[string]StrategyStats
	recent        ringBuffer
}

type StrategyStats struct {
	Count              int
	CumulativeDuration time.Duration
	TotalPostings      int
	MaxDuration        time.Duration
}

func (s StrategyStats) AvgDuration() time.Duration {
	if s.Count == 0 {
		return 0
	}
	return s.CumulativeDuration / time.Duration(s.Count)
}

func (s StrategyStats) AvgPostings() float64 {
	if s.Count == 0 {
		return 0
	}
	return float64(s.TotalPostings) / float64(s.Count)
}

type SearchEvent struct {
	At                 time.Time
	QueryHash          string
	LogicalQueryType   string
	ExecutionStrategy  string
	StrategySkipReason string
	TotalDuration      time.Duration
	PostingEntriesRead int
	MatchedDocs        int
	ReturnedDocs       int
	Error              string
}

type Snapshot struct {
	TotalSearches int
	ErrorsTotal   int
	ZeroResults   int
	ByStrategy    map[string]StrategyStats
	Recent        []SearchEvent
}

func NewSearchStats(recentLimit int) *SearchStats {
	if recentLimit <= 0 {
		recentLimit = defaultRecentLimit
	}
	return &SearchStats{
		byStrategy: make(map[string]StrategyStats),
		recent:     newRingBuffer(recentLimit),
	}
}

func (s *SearchStats) ObserveSearch(query string, d *fts.QueryDiagnostics, err error) {
	if s == nil {
		return
	}

	event := SearchEvent{At: time.Now(), QueryHash: queryHash(query)}
	if d != nil {
		event.LogicalQueryType = d.LogicalQueryType
		event.ExecutionStrategy = d.ExecutionStrategy
		event.StrategySkipReason = d.StrategySkipReason
		event.TotalDuration = d.Timings["total"]
		event.PostingEntriesRead = d.PostingEntriesRead
		event.MatchedDocs = d.MatchedDocs
		event.ReturnedDocs = d.ReturnedDocs
	}
	if err != nil {
		event.Error = err.Error()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalSearches++
	if err != nil {
		s.errorsTotal++
	}
	if d != nil && d.MatchedDocs == 0 {
		s.zeroResults++
	}
	if d != nil {
		strategy := d.ExecutionStrategy
		if strategy == "" {
			strategy = "unknown"
		}
		st := s.byStrategy[strategy]
		st.Count++
		st.CumulativeDuration += d.Timings["total"]
		st.TotalPostings += d.PostingEntriesRead
		if d.Timings["total"] > st.MaxDuration {
			st.MaxDuration = d.Timings["total"]
		}
		s.byStrategy[strategy] = st
	}
	s.recent.add(event)
}

func (s *SearchStats) Snapshot() Snapshot {
	if s == nil {
		return Snapshot{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := Snapshot{
		TotalSearches: s.totalSearches,
		ErrorsTotal:   s.errorsTotal,
		ZeroResults:   s.zeroResults,
		ByStrategy:    make(map[string]StrategyStats, len(s.byStrategy)),
		Recent:        s.recent.snapshot(0),
	}
	for k, v := range s.byStrategy {
		out.ByStrategy[k] = v
	}
	return out
}

func (s *SearchStats) Recent(limit int) []SearchEvent {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recent.snapshot(limit)
}

func queryHash(query string) string {
	if query == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(query))
	return hex.EncodeToString(sum[:8])
}

type ringBuffer struct {
	items  []SearchEvent
	next   int
	filled bool
}

func newRingBuffer(size int) ringBuffer {
	if size <= 0 {
		size = defaultRecentLimit
	}
	return ringBuffer{items: make([]SearchEvent, size)}
}

func (r *ringBuffer) add(ev SearchEvent) {
	if len(r.items) == 0 {
		return
	}
	r.items[r.next] = ev
	r.next = (r.next + 1) % len(r.items)
	if r.next == 0 {
		r.filled = true
	}
}

func (r *ringBuffer) snapshot(limit int) []SearchEvent {
	size := r.size()
	if size == 0 {
		return nil
	}
	if limit <= 0 || limit > size {
		limit = size
	}
	out := make([]SearchEvent, 0, limit)
	for i := 0; i < limit; i++ {
		idx := (r.next - 1 - i + len(r.items)) % len(r.items)
		out = append(out, r.items[idx])
	}
	return out
}

func (r *ringBuffer) size() int {
	if r.filled {
		return len(r.items)
	}
	return r.next
}
