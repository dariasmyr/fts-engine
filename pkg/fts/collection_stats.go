package fts

import "sync"

type CollectionStatsSnapshot struct {
	DocsSeen map[DocID]bool
	DocLen   map[string]map[DocID]uint32
	TotalLen map[string]uint64
}

type collectionStats struct {
	mu       sync.RWMutex
	docsSeen map[DocOrd]struct{}
	docLen   map[string]map[DocOrd]uint32
	totalLen map[string]uint64
}

func newCollectionStats() *collectionStats {
	return &collectionStats{
		docsSeen: make(map[DocOrd]struct{}),
		docLen:   make(map[string]map[DocOrd]uint32),
		totalLen: make(map[string]uint64),
	}
}

func newCollectionStatsFromSnapshot(snapshot *CollectionStatsSnapshot, registry *DocRegistry) *collectionStats {
	stats := newCollectionStats()
	if snapshot == nil {
		return stats
	}
	for id, seen := range snapshot.DocsSeen {
		if seen {
			stats.docsSeen[registry.GetOrAssign(id)] = struct{}{}
		}
	}
	for field, perField := range snapshot.DocLen {
		copied := make(map[DocOrd]uint32, len(perField))
		for id, tokens := range perField {
			copied[registry.GetOrAssign(id)] = tokens
		}
		stats.docLen[field] = copied
	}
	for field, total := range snapshot.TotalLen {
		stats.totalLen[field] = total
	}
	return stats
}

func (c *collectionStats) observe(field string, ord DocOrd, tokens uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, seen := c.docsSeen[ord]; !seen {
		c.docsSeen[ord] = struct{}{}
	}

	perField, ok := c.docLen[field]
	if !ok {
		perField = make(map[DocOrd]uint32)
		c.docLen[field] = perField
	}
	perField[ord] += tokens
	c.totalLen[field] += uint64(tokens)
}

func (c *collectionStats) TotalDocs() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.docsSeen)
}

func (c *collectionStats) snapshot(registry *DocRegistry) *CollectionStatsSnapshot {
	if c == nil {
		return nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	snapshot := &CollectionStatsSnapshot{
		DocsSeen: make(map[DocID]bool, len(c.docsSeen)),
		DocLen:   make(map[string]map[DocID]uint32, len(c.docLen)),
		TotalLen: make(map[string]uint64, len(c.totalLen)),
	}
	for ord := range c.docsSeen {
		id := registry.Lookup(ord)
		if id == "" {
			continue
		}
		snapshot.DocsSeen[id] = true
	}
	for field, perField := range c.docLen {
		copied := make(map[DocID]uint32, len(perField))
		for ord, tokens := range perField {
			id := registry.Lookup(ord)
			if id == "" {
				continue
			}
			copied[id] = tokens
		}
		snapshot.DocLen[field] = copied
	}
	for field, total := range c.totalLen {
		snapshot.TotalLen[field] = total
	}
	return snapshot
}

func (c *collectionStats) DocLen(field string, ord DocOrd) uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if perField, ok := c.docLen[field]; ok {
		return perField[ord]
	}
	return 0
}

func (c *collectionStats) AvgDocLen(field string) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	perField, ok := c.docLen[field]
	if !ok || len(perField) == 0 {
		return 0
	}
	return float64(c.totalLen[field]) / float64(len(perField))
}

func (c *collectionStats) FieldDocCount(field string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if perField, ok := c.docLen[field]; ok {
		return len(perField)
	}
	return 0
}
