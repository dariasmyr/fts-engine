package loader

import (
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fts-hw/internal/domain/models"
	"io"
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

// LoadDocuments loads a Wikipedia abstract dump and returns a slice of documents.
// Dump example: https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract1.xml.gz
func (l *Loader) LoadDocuments() ([]models.Document, error) {
	f, err := os.Open(l.dumpPath)
	if err != nil {
		l.log.Error("Failed to open file", "error", err)
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
		Documents []models.Document `xml:"doc"`
	}{}

	if err := dec.Decode(&dump); err != nil {
		return nil, err
	}

	for i := range dump.Documents {
		dump.Documents[i].ID = l.generateID(dump.Documents[i])
	}

	return dump.Documents, nil
}

func (l *Loader) ChunkDocuments(documents []models.Document, chunkSize int) [][]models.Document {
	numChunks := (len(documents) + chunkSize - 1) / chunkSize
	chunks := make([][]models.Document, numChunks)

	for i := 0; i < numChunks; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(documents) {
			end = len(documents)
		}
		chunks[i] = documents[i:end]
	}

	return chunks
}

func (l *Loader) generateID(document models.Document) string {
	hasher := md5.New()
	io.WriteString(hasher, document.Title+"|"+document.URL+"|"+document.Text)
	return hex.EncodeToString(hasher.Sum(nil))
}
