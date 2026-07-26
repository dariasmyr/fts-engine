package models

type DocumentBase struct {
	Title    string `xml:"title" json:"title"`
	URL      string `xml:"url" json:"url"`
	Abstract string `xml:"abstract" json:"abstract"`
}

type Document struct {
	DocumentBase
	ID      string `json:"id"`
	Extract string `json:"extract"`
}

type ResultData struct {
	ID            string   `json:"id"`
	UniqueMatches int      `json:"unique_matches"`
	TotalMatches  int      `json:"total_matches"`
	Score         float64  `json:"score"`
	Explanation   *Explain `json:"explanation,omitempty"`
	Document      Document `json:"document"`
}

type Explain struct {
	Score         float64             `json:"score"`
	UniqueMatches int                 `json:"unique_matches"`
	TotalMatches  int                 `json:"total_matches"`
	Contributions []ScoreContribution `json:"contributions"`
}

type ScoreContribution struct {
	Field           string  `json:"field"`
	Term            string  `json:"term"`
	QueryType       string  `json:"query_type"`
	TF              uint32  `json:"tf"`
	DF              uint32  `json:"df"`
	BaseScore       float64 `json:"base_score"`
	FieldWeight     float64 `json:"field_weight"`
	QueryTypeWeight float64 `json:"query_type_weight"`
	Score           float64 `json:"score"`
}

type SearchDiagnostics struct {
	LogicalQueryType   string            `json:"logical_query_type"`
	ExecutionStrategy  string            `json:"execution_strategy"`
	StrategySkipReason string            `json:"strategy_skip_reason"`
	Timings            map[string]string `json:"timings"`
	ProcessedTokens    int               `json:"processed_tokens"`
	FieldsVisited      int               `json:"fields_visited"`
	GeneratedKeys      int               `json:"generated_keys"`
	IndexSearches      int               `json:"index_searches"`
	FilterChecks       int               `json:"filter_checks"`
	FilterRejects      int               `json:"filter_rejects"`
	PostingEntriesRead int               `json:"posting_entries_read"`
	CandidateDocs      int               `json:"candidate_docs"`
	MatchedDocs        int               `json:"matched_docs"`
	ReturnedDocs       int               `json:"returned_docs"`
}

type SearchResult struct {
	ResultData        []ResultData
	TotalResultsCount int
	Diagnostics       *SearchDiagnostics
}
