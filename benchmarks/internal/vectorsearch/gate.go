package vectorsearch

// QualityGate describes the minimum quality required for one dataset, metric,
// and effective ANN search budget.
type QualityGate struct {
	Dataset              DatasetKind
	Metric               string
	EffectiveEfSearch    int
	MinRecallAtK         float64
	MinDocumentRecallAtK float64
	RequireComplete      bool
}

type QualityGateFailure struct {
	Run      RunReport
	Metric   string
	Got      float64
	Expected float64
}

// CheckQualityGates evaluates matching runs independently. Runs without a
// configured gate are intentionally left unconstrained.
func CheckQualityGates(report Report, gates []QualityGate) []QualityGateFailure {
	var failures []QualityGateFailure
	for _, run := range report.Runs {
		for _, gate := range gates {
			if run.Dataset.Kind != gate.Dataset || run.Dataset.Metric != gate.Metric || run.Request.EffectiveEfSearch != gate.EffectiveEfSearch {
				continue
			}
			if run.Quality.MeanRecallAtK < gate.MinRecallAtK {
				failures = append(failures, QualityGateFailure{Run: run, Metric: "recall@k", Got: run.Quality.MeanRecallAtK, Expected: gate.MinRecallAtK})
			}
			if run.Quality.MeanDocumentRecallAtK < gate.MinDocumentRecallAtK {
				failures = append(failures, QualityGateFailure{Run: run, Metric: "document-recall@k", Got: run.Quality.MeanDocumentRecallAtK, Expected: gate.MinDocumentRecallAtK})
			}
			if gate.RequireComplete && run.SearchOutcome.IncompleteRate > 0 {
				failures = append(failures, QualityGateFailure{Run: run, Metric: "incomplete-rate", Got: run.SearchOutcome.IncompleteRate, Expected: 0})
			}
		}
	}
	return failures
}
