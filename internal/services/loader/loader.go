package loader

import (
	"compress/gzip"
	"encoding/xml"
	"log/slog"
	"os"
)

type Loader struct {
	log      *slog.Logger
	dumpPath string
}

func NewLoader(log *slog.Logger, dumpPath string) *Loader {
	return &Loader{
		log:      log,
		dumpPath: dumpPath,
	}
}

// Document represents a Wikipedia abstract dump document.
type Document struct {
	Title string `xml:"title"`
	URL   string `xml:"url"`
	Text  string `xml:"abstract"`
	ID    int
}

// LoadDocuments loads a Wikipedia abstract dump and returns a slice of documents.
// Dump example: https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract1.xml.gz
func (l *Loader) LoadDocuments() ([]Document, error) {
	f, err := os.Open(l.dumpPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	dec := xml.NewDecoder(gz)
	dump := struct {
		Documents []Document `xml:"doc"`
	}{}
	if err := dec.Decode(&dump); err != nil {
		return nil, err
	}
	docs := dump.Documents
	for i := range docs {
		docs[i].ID = i
	}
	return docs, nil
}
