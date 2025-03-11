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
