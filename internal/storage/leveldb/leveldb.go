package leveldb

import (
	"context"
	"encoding/json"
	"fmt"
	"fts-hw/internal/domain/models"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
)

type Storage struct {
	db *leveldb.DB
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func NewStorage(path string) (*Storage, error) {
	const op = "storage.leveldb.New"

	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Storage{db: db}, nil
}

func (s *Storage) GetDatabaseStats(context context.Context) (string, error) {
	stats, err := s.db.GetProperty("leveldb.stats")
	if err != nil {
		return "", err
	}

	return stats, nil
}

func (s *Storage) SaveDocumentWithIndexing(context context.Context, document *models.Document, words []string) (string, error) {
	batch := new(leveldb.Batch)

	data, err := json.Marshal(document)
	if err != nil {
		return "", err
	}

	// Save the document content
	batch.Put([]byte("doc:"+document.ID), data)

	// Word indexing
	wordsCount := make(map[string]int)
	for _, word := range words {
		wordsCount[word]++
	}

	for word, count := range wordsCount {
		wordKey := "word:" + word
		var indexDataBuilder strings.Builder

		existing, err := s.db.Get([]byte(wordKey), nil)

		if err == nil && len(existing) > 0 {
			indexDataBuilder.Write(existing)
			indexDataBuilder.WriteByte(',')
		}

		indexDataBuilder.WriteString(fmt.Sprintf("%s:%d", document.ID, count)) // append the new index

		// Save the updated index data for the word
		batch.Put([]byte(wordKey), []byte(indexDataBuilder.String()))
	}

	// Apply all batch operations
	err = s.db.Write(batch, nil)
	if err != nil {
		return "", err
	}

	return document.ID, nil
}

func (s *Storage) SaveDocument(context context.Context, document *models.Document) (string, error) {
	batch := new(leveldb.Batch)

	data, err := json.Marshal(document)
	if err != nil {
		return "", err
	}

	// Save the document content
	batch.Put([]byte("doc:"+document.ID), data)

	// Apply all batch operations
	err = s.db.Write(batch, nil)
	if err != nil {
		return "", err
	}

	return document.ID, nil
}

func (s *Storage) GetWord(cxt context.Context, word string) ([]string, error) {
	wordKey := "word:" + word
	data, err := s.db.Get([]byte(wordKey), nil)
	if err != nil {
		return nil, fmt.Errorf("word %s not found", word)
	}

	return strings.Split(string(data), ","), nil
}

func (s *Storage) GetDocument(cxt context.Context, docID string) (*models.Document, error) {
	data, err := s.db.Get([]byte("doc:"+docID), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}

	var doc models.Document
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}

	return &doc, nil
}

func (s *Storage) DeleteDocument(context context.Context, docID string) error {
	batch := new(leveldb.Batch)

	docKey := "doc:" + docID
	batch.Delete([]byte(docKey))

	// Run over all indexes and delete references to document
	iter := s.db.NewIterator(nil, nil)
	for iter.Next() {
		key := string(iter.Key())

		if strings.HasPrefix(key, "word:") {
			value := string(iter.Value())
			entries := strings.Split(value, ",")
			var newEntries []string
			for _, entry := range entries {
				parts := strings.Split(entry, ":")
				id := parts[0]
				if id != docID {
					newEntries = append(newEntries, entry)
				}
			}

			// If word is in other documents - update, otherwise delete
			if len(newEntries) > 0 {
				batch.Put([]byte(key), []byte(strings.Join(newEntries, ",")))
			} else {
				batch.Delete([]byte(key))
			}
		}
	}

	iter.Release()

	return s.db.Write(batch, nil)
}
