package app

import (
	"fts-hw/internal/services/fts"
	"log/slog"
)

type App struct {
	App        *fts.FTS
	StorageApp *StorageApp
}

func New(
	log *slog.Logger,
	storagePath string,
) *App {
	storageApp, err := NewStorageApp(storagePath)
	if err != nil {
		panic(err)
	}

	ftsService := fts.New(log, storageApp.Storage(), storageApp.Storage())

	return &App{
		App:        ftsService,
		StorageApp: storageApp,
	}
}
