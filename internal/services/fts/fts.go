package fts

import (
	"context"
	"errors"
	"log/slog"
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
	AddDocument(ctx context.Context, content string) (int, error)
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
