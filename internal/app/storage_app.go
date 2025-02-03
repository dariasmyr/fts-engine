package app

import (
	"fts-hw/internal/storage/leveldb"
)

type StorageApp struct {
	storage *leveldb.Storage
}

func NewStorageApp(storagePath string) (*StorageApp, error) {
	storage, err := leveldb.NewStorage(storagePath)
	if err != nil {
		return nil, err
	}
	return &StorageApp{storage: storage}, nil
}

func (s *StorageApp) Stop() error {
	return s.storage.Close()
}

func (s *StorageApp) Storage() *leveldb.Storage {
	return s.storage
}
