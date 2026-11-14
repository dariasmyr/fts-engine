package service

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
)

type DocumentEncoder interface {
	EncodeDocument(context.Context, fts.Document) ([]semantic.ChunkEmbedding, error)
}

type QueryEncoder interface {
	EncodeQuery(context.Context, string) ([]semantic.ChunkEmbedding, error)
	EncodeQueryDocument(context.Context, fts.Document) ([]semantic.ChunkEmbedding, error)
}
