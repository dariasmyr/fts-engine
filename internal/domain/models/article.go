package models

type Article struct {
	PageID  int    `json:"pageid"`
	NS      int    `json:"ns"`
	Title   string `json:"title"`
	Extract string `json:"extract"`
}

type APIResponse struct {
	BatchComplete string `json:"batchcomplete"`
	Query         Query  `json:"query"`
}

type Query struct {
	Normalized []NormalizedEntry  `json:"normalized"`
	Pages      map[string]Article `json:"pages"`
}

type NormalizedEntry struct {
	From string `json:"from"`
	To   string `json:"to"`
}
