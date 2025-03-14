package leveldb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"fts-hw/internal/domain/models"
	"fts-hw/internal/lib/logger/sl"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

type Storage struct {
	log       *slog.Logger
	db        *leveldb.DB
	writeChan chan *models.Document
	wg        sync.WaitGroup
}

var ErrNotFound = errors.New("doc not found")

const (
	bufferSize   = 1000
	flushTimeout = 2 * time.Second
)

func NewStorage(log *slog.Logger, path string) (*Storage, error) {
	const op = "storage.leveldb.New"

	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	storage := &Storage{
		log:       log,
		db:        db,
		writeChan: make(chan *models.Document, bufferSize*2),
	}

	storage.wg.Add(1)
	go storage.writeWorker()

	return storage, nil
}

func (s *Storage) writeWorker() {
	defer s.wg.Done()

	batch := new(leveldb.Batch)
	ticker := time.NewTicker(flushTimeout)
	defer ticker.Stop()

	for {
		select {
		case doc, ok := <-s.writeChan:
			if !ok {
				fmt.Println("Channel closed, flushing batch", batch.Len())
				err := s.db.Write(batch, nil)
				if err != nil {
					s.log.Error("Failed to write batch", "error", sl.Err(err))
				}
				return
			}

			data, _ := json.Marshal(doc)
			batch.Put([]byte("doc:"+doc.ID), data)

			if batch.Len() >= bufferSize {
				fmt.Printf("Flushing batch, len: %d\n", batch.Len())
				err := s.db.Write(batch, nil)
				if err != nil {
					s.log.Error("Failed to write batch", "error", sl.Err(err))
				}
				batch = new(leveldb.Batch)
			}

		case <-ticker.C:
			if batch.Len() > 0 {
				fmt.Println("Timeout, flushing batch, len: ", batch.Len())
				err := s.db.Write(batch, nil)
				if err != nil {
					s.log.Error("Failed to write batch", "error", sl.Err(err))
				}
				batch = new(leveldb.Batch)
			}
		}
	}
}

func (s *Storage) GetDatabaseStats(context context.Context) (string, error) {
	stats, err := s.db.GetProperty("leveldb.stats")
	if err != nil {
		return "", err
	}

	return stats, nil
}

func (s *Storage) SaveWordsWithIndexing(context context.Context, document *models.Document, words []string) (int, error) {
	batch := new(leveldb.Batch)

	// Word indexing
	wordsCount := make(map[string]int)
	for _, word := range words {
		wordsCount[word]++
	}

	successfulCount := 0
	for word, count := range wordsCount {
		wordKey := "word:" + word
		var indexDataBuilder strings.Builder

		existing, err := s.db.Get([]byte(wordKey), nil)

		if err == nil && len(existing) > 0 {
			indexDataBuilder.Write(existing)
			indexDataBuilder.WriteByte(',')
			successfulCount++
		}

		indexDataBuilder.WriteString(fmt.Sprintf("%s:%d", document.ID, count)) // append the new index

		// Save the updated index data for the word
		batch.Put([]byte(wordKey), []byte(indexDataBuilder.String()))
	}

	// Apply all batch operations
	err := s.db.Write(batch, nil)
	if err != nil {
		return 0, err
	}

	return successfulCount, nil
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

func (s *Storage) BatchDocument(context context.Context, document *models.Document) (string, error) {
	select {
	case s.writeChan <- document:
		return document.ID, nil
	case <-context.Done():
		return "", context.Err()
	}
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
			return nil, err
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

func (s *Storage) Close() error {
	return s.db.Close()
}

func (s *Storage) StopWorkers() {
	close(s.writeChan)
	s.wg.Wait()
}
