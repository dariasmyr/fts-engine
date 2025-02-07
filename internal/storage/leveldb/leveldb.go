package leveldb

import (
	"context"
	"fmt"
	"strconv"
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

func (s *Storage) AddDocument(context context.Context, content string, words []string) (int, error) {
	batch := new(leveldb.Batch)

	// Retrieve the last document ID
	lastIDBytes, err := s.db.Get([]byte("doc_counter"), nil)
	var lastID int
	if err == nil {
		lastID, _ = strconv.Atoi(string(lastIDBytes))
	}

	newID := lastID + 1
	newIDStr := strconv.Itoa(newID)

	// Update document counter
	batch.Put([]byte("doc_counter"), []byte(newIDStr))

	// Save the document content
	batch.Put([]byte("doc:"+newIDStr), []byte(content))

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

		indexDataBuilder.WriteString(fmt.Sprintf("%d:%d", newID, count)) // append the new index

		// Save the updated index data for the word
		batch.Put([]byte(wordKey), []byte(indexDataBuilder.String()))
	}

	// Apply all batch operations
	err = s.db.Write(batch, nil)
	if err != nil {
		return 0, err
	}

	return newID, nil
}

func (s *Storage) SearchWord(cxt context.Context, word string) ([]string, error) {
	wordKey := "word:" + word
	data, err := s.db.Get([]byte(wordKey), nil)
	if err != nil {
		return nil, fmt.Errorf("word %s not found", word)
	}

	return strings.Split(string(data), ","), nil
}

func (s *Storage) SearchDocument(cxt context.Context, docID int) (string, error) {
	docKey := "doc:" + strconv.Itoa(docID)

	docData, err := s.db.Get([]byte(docKey), nil)
	if err != nil {
		return "", fmt.Errorf("document ID %d not found", docID)
	}

	return string(docData), nil
}

func (s *Storage) DeleteDocument(context context.Context, docID int) error {
	batch := new(leveldb.Batch)

	docKey := "doc:" + strconv.Itoa(docID)
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
				id, _ := strconv.Atoi(parts[0])
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
