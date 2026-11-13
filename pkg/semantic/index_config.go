package semantic

import (
	"math"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

const defaultMaxSegments = 16

// IndexConfig is the complete frozen contract of an Index.
type IndexConfig struct {
	Dimensions  int
	Metric      vector.Metric
	Build       hnsw.BuildConfig
	Operational OperationalLimits
}

// OperationalLimits groups the frozen capacity, ingestion, and search policy.
type OperationalLimits struct {
	Capacity       CapacityLimits
	Ingestion      IngestionLimits
	Search         SearchLimits
	SearchDefaults hnsw.SearchConfig
}

// CapacityLimits bounds physical and visible index storage.
type CapacityLimits struct {
	MaxLiveVectors  int
	MaxStaleVectors int
	MaxSegments     int
}

// IngestionLimits bounds one document mutation.
type IngestionLimits struct {
	MaxChunksPerDocument int
}

// SearchLimits bounds request-local search budgets and result shape.
type SearchLimits struct {
	MaxResults         int
	MaxCandidateChunks int
	MaxChunksPerResult int
	MaxQueryChunks     int
	MaxEfSearch        int
	MaxVisitLimit      int
}

// Validate checks whether c can be normalized into a valid index contract.
func (c IndexConfig) Validate() error { _, err := c.normalized(); return err }

func (c IndexConfig) validateCanonical() error {
	normalized, err := c.normalized()
	if err != nil || normalized != c {
		return ErrInvalidConfig
	}
	return nil
}

// ValidateCanonical checks that a persisted config is valid without applying defaults.
func (c IndexConfig) ValidateCanonical() error { return c.validateCanonical() }

func (c IndexConfig) normalized() (IndexConfig, error) {
	capacity := &c.Operational.Capacity
	ingestion := c.Operational.Ingestion
	search := &c.Operational.Search
	defaults := &c.Operational.SearchDefaults
	if capacity.MaxLiveVectors <= 0 || capacity.MaxStaleVectors < 0 || capacity.MaxSegments < 0 ||
		ingestion.MaxChunksPerDocument <= 0 || ingestion.MaxChunksPerDocument > capacity.MaxLiveVectors ||
		search.MaxResults <= 0 || search.MaxCandidateChunks < search.MaxResults || search.MaxChunksPerResult <= 0 ||
		search.MaxQueryChunks < 0 || search.MaxEfSearch < 0 || search.MaxVisitLimit < 0 {
		return IndexConfig{}, ErrInvalidConfig
	}
	if capacity.MaxStaleVectors == 0 {
		capacity.MaxStaleVectors = capacity.MaxLiveVectors
	}
	if capacity.MaxSegments == 0 {
		capacity.MaxSegments = defaultMaxSegments
	}
	if c.Build.MaxNeighbors == 0 {
		c.Build.MaxNeighbors = 16
	}
	if c.Build.EfConstruction == 0 {
		c.Build.EfConstruction = max(64, c.Build.MaxNeighbors)
	}
	if defaults.EfSearch == 0 {
		defaults.EfSearch = max(search.MaxResults, min(search.MaxCandidateChunks, 64))
		if search.MaxEfSearch > 0 {
			defaults.EfSearch = min(defaults.EfSearch, search.MaxEfSearch)
		}
	}
	if defaults.VisitLimit == 0 {
		defaults.VisitLimit = capacity.MaxLiveVectors
		if search.MaxVisitLimit > 0 {
			defaults.VisitLimit = min(defaults.VisitLimit, search.MaxVisitLimit)
		}
	}
	if search.MaxQueryChunks == 0 {
		search.MaxQueryChunks = ingestion.MaxChunksPerDocument
	}
	if search.MaxEfSearch == 0 {
		search.MaxEfSearch = max(search.MaxCandidateChunks, defaults.EfSearch)
	}
	if search.MaxVisitLimit == 0 {
		search.MaxVisitLimit = capacity.MaxLiveVectors
	}
	if c.Dimensions <= 0 || !c.Metric.Valid() || capacity.MaxSegments <= 0 ||
		search.MaxCandidateChunks > capacity.MaxLiveVectors || search.MaxChunksPerResult > ingestion.MaxChunksPerDocument ||
		defaults.EfSearch > search.MaxEfSearch || defaults.VisitLimit > search.MaxVisitLimit {
		return IndexConfig{}, ErrInvalidConfig
	}
	for _, value := range []int{c.Dimensions, capacity.MaxLiveVectors, capacity.MaxStaleVectors, capacity.MaxSegments,
		ingestion.MaxChunksPerDocument, search.MaxResults, search.MaxCandidateChunks, search.MaxChunksPerResult,
		search.MaxQueryChunks, search.MaxEfSearch, search.MaxVisitLimit, c.Build.MaxNeighbors,
		c.Build.EfConstruction, defaults.EfSearch, defaults.VisitLimit} {
		if value <= 0 || uint64(value) > math.MaxUint32 {
			return IndexConfig{}, ErrInvalidConfig
		}
	}
	if err := c.hnswConfig().Validate(); err != nil {
		return IndexConfig{}, ErrInvalidConfig
	}
	maxInt := int(^uint(0) >> 1)
	if capacity.MaxLiveVectors > maxInt-capacity.MaxStaleVectors ||
		!validVectorCapacity(c.Dimensions, capacity.MaxLiveVectors+capacity.MaxStaleVectors) {
		return IndexConfig{}, ErrInvalidConfig
	}
	if err := c.runtimeLimits().validate(); err != nil {
		return IndexConfig{}, err
	}
	return c, nil
}

func (c IndexConfig) hnswConfig() hnsw.Config {
	return hnsw.Config{Build: c.Build, Search: c.Operational.SearchDefaults}
}

func validVectorCapacity(dimensions, maxVectors int) bool {
	if dimensions <= 0 || maxVectors <= 0 {
		return false
	}
	d, n := uint64(dimensions), uint64(maxVectors)
	return d <= math.MaxUint64/4 && n <= (math.MaxUint64-128)/(d*4)
}

type capacityLimits struct{ maxLiveVectors, maxStaleVectors int }
type lifecycleLimits struct{ maxSegments int }
type ingestionLimits struct{ maxChunksPerDocument int }
type searchBudgetLimits struct{ maxChunkCandidates, maxQueryChunks, maxEfSearch, maxVisitLimit int }
type resultShapeLimits struct{ maxDocumentsPerSearch, maxChunksPerDocumentHit int }
type indexLimits struct {
	capacity  capacityLimits
	lifecycle lifecycleLimits
	ingestion ingestionLimits
	search    searchBudgetLimits
	results   resultShapeLimits
}

func (c IndexConfig) runtimeLimits() indexLimits {
	o := c.Operational
	return indexLimits{
		capacity:  capacityLimits{o.Capacity.MaxLiveVectors, o.Capacity.MaxStaleVectors},
		lifecycle: lifecycleLimits{o.Capacity.MaxSegments},
		ingestion: ingestionLimits{o.Ingestion.MaxChunksPerDocument},
		search:    searchBudgetLimits{o.Search.MaxCandidateChunks, o.Search.MaxQueryChunks, o.Search.MaxEfSearch, o.Search.MaxVisitLimit},
		results:   resultShapeLimits{o.Search.MaxResults, o.Search.MaxChunksPerResult},
	}
}

func (l indexLimits) validate() error {
	if l.capacity.maxLiveVectors <= 0 || l.capacity.maxStaleVectors < 0 || l.lifecycle.maxSegments <= 0 ||
		l.ingestion.maxChunksPerDocument <= 0 || l.search.maxChunkCandidates < l.results.maxDocumentsPerSearch ||
		l.search.maxQueryChunks <= 0 || l.search.maxEfSearch <= 0 || l.search.maxVisitLimit <= 0 ||
		l.results.maxDocumentsPerSearch <= 0 || l.results.maxChunksPerDocumentHit <= 0 {
		return ErrInvalidConfig
	}
	return nil
}
