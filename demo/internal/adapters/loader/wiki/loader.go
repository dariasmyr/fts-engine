package wiki

import (
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"io"
	"log/slog"
	"os"

	"github.com/dariasmyr/fts-engine/demo/internal/domain/models"
)

type Loader struct {
	log          *slog.Logger
	dumpPath     string
	maxDocuments int
}

func New(log *slog.Logger, dumpPath string, maxDocuments int) *Loader {
	return &Loader{log: log, dumpPath: dumpPath, maxDocuments: maxDocuments}
}

func (l *Loader) LoadDocuments(ctx context.Context) (documents []models.Document, err error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	f, err := os.Open(l.dumpPath)
	if err != nil {
		l.log.Error("Failed to open file", "error", err)
		return nil, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			l.log.Error("Failed to close file", "error", closeErr)
			if err == nil {
				err = closeErr
			}
		}
	}()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := gz.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	dec := xml.NewDecoder(gz)
	documents = make([]models.Document, 0, initialCapacity(l.maxDocuments))
	for l.maxDocuments == 0 || len(documents) < l.maxDocuments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		token, decodeErr := dec.Token()
		if decodeErr == io.EOF {
			break
		}
		if decodeErr != nil {
			return nil, decodeErr
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "doc" {
			continue
		}
		var document models.Document
		if decodeErr := dec.DecodeElement(&document, &start); decodeErr != nil {
			return nil, decodeErr
		}
		document.ID = l.generateID(document)
		documents = append(documents, document)
	}

	return documents, nil
}

func initialCapacity(maxDocuments int) int {
	if maxDocuments > 0 {
		return min(maxDocuments, 25_000)
	}
	return 0
}

func (l *Loader) generateID(document models.Document) string {
	hasher := md5.New()
	io.WriteString(hasher, document.Title+"|"+document.URL+"|"+document.Abstract)
	return hex.EncodeToString(hasher.Sum(nil))
}
