package fts

import (
	"context"
	"errors"
	"log/slog"
	"strings"
)

type FTS struct {
	log              *slog.Logger
	documentSaver    DocumentSaver
	documentProvider DocumentProvider
}

var (
	ErrInvalidCredentials = errors.New("invalid credentials")
)

type DocumentSaver interface {
	AddDocument(ctx context.Context, content string, words []string) (int, error)
	DeleteDocument(ctx context.Context, docId int) error
}

type DocumentProvider interface {
	Search(ctx context.Context, word string) ([]string, error)
}

func New(
	log *slog.Logger,
	documentSaver DocumentSaver,
	documentProvider DocumentProvider,
) *FTS {
	return &FTS{
		log:              log,
		documentSaver:    documentSaver,
		documentProvider: documentProvider,
	}
}

func (fts *FTS) PreprocessText(content string) []string {
	// TODO: Add tokenization and stemming

	words := strings.Fields(content)

	return words
}

func (fts *FTS) AddDocument(ctx context.Context, content string) (int, error) {
	words := fts.PreprocessText(content)
	return fts.documentSaver.AddDocument(ctx, content, words)
}
