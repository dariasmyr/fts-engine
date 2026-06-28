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
	Document      Document `json:"document"`
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
