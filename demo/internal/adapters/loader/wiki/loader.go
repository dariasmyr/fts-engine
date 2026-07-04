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
	log      *slog.Logger
	dumpPath string
}

func New(log *slog.Logger, dumpPath string) *Loader {
	return &Loader{log: log, dumpPath: dumpPath}
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
		err = f.Close()
		if err != nil {
			l.log.Error("Failed to close file", "error", err)
		}
	}()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = gz.Close()
	}()

	dec := xml.NewDecoder(gz)
	dump := struct {
		Documents []models.Document `xml:"doc"`
	}{}

	if decodeErr := dec.Decode(&dump); decodeErr != nil {
		return nil, decodeErr
	}

	for i := range dump.Documents {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			dump.Documents[i].ID = l.generateID(dump.Documents[i])
		}
	}

	return dump.Documents, nil
}

func (l *Loader) generateID(document models.Document) string {
	hasher := md5.New()
	io.WriteString(hasher, document.Title+"|"+document.URL+"|"+document.Abstract)
	return hex.EncodeToString(hasher.Sum(nil))
}
