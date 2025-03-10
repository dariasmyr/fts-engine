package models

type Document struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	Extract string `json:"extract"`
}

func NewDocument(id string, title string, extract string) *Document {
	return &Document{
		ID:      id,
		Title:   title,
		Extract: extract,
	}
}
