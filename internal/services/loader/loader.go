package loader

import (
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"fts-hw/internal/domain/models"
	"fts-hw/internal/lib/logger/sl"
	utils "fts-hw/internal/utils/clean"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
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
	io.WriteString(hasher, document.Title+"|"+document.URL+"|"+document.Abstract)
	return hex.EncodeToString(hasher.Sum(nil))
}

func (l *Loader) parseUrl(doc models.Document) (host string, title string, err error) {
	parsedURL, err := url.Parse(doc.URL)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse URL: %v", err)
	}

	var hostBuilder strings.Builder

	hostBuilder.WriteString(parsedURL.Scheme)
	hostBuilder.WriteString("://")
	hostBuilder.WriteString(parsedURL.Host)

	host = hostBuilder.String()

	title = strings.TrimPrefix(parsedURL.Path, "/wiki/")

	return host, title, nil
}

func (l *Loader) FetchAndProcessDocument(ctx context.Context, doc models.Document) (models.Document, error) {
	host, title, err := l.parseUrl(doc)
	if err != nil {
		l.log.Error("Error parsing url", "error", sl.Err(err))
		return doc, err
	}

	apiURL := fmt.Sprintf("%s/w/api.php?action=query&prop=extracts&explaintext=true&format=json&titles=%s", host, title)
	resp, err := http.Get(apiURL)
	if err != nil {
		l.log.Error("Error getting url", "error", sl.Err(err))
		return doc, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		l.log.Error("Error reading body", "error", sl.Err(err))
		return doc, err
	}

	var apiResponse models.ArticleResponse
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		l.log.Error("Error unmarshalling body", "error", sl.Err(err))
		return doc, err
	}

	for _, page := range apiResponse.Query.Pages {
		if page.Extract == "" {
			l.log.Error("Empty extract")
			return doc, errors.New("empty extract in response")
		}

		doc.Extract = utils.Clean(page.Extract)
	}

	return doc, nil
}
