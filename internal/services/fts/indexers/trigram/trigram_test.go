package trigramtrie

import (
	"context"
	"fmt"
	"fts-hw/internal/domain/models"
	"fts-hw/internal/services/fts"
	"fts-hw/internal/storage/leveldb"
	"log/slog"
	"os"
	"reflect"
	"sync"
	"testing"
)

var documents = []models.Document{{
	DocumentBase: models.DocumentBase{
		Title:    "Wikipedia: Sans Souci Hotel (Ballston Spa)",
		URL:      "https://en.wikipedia.org/wiki/Sans_Souci_Hotel_(Ballston_Spa)",
		Abstract: "Wikipedia: The Sans Souci Hotel was a hotel located in Ballston Spa, Saratoga County, New York. It was built in 1803, closed as a hotel in 1849, and the building, used for other purposes, was torn down in 1887.",
	},
	Extract: "",
	ID:      "1",
},
	{
		DocumentBase: models.DocumentBase{
			Title:    "Wikipedia: Hotellet",
			URL:      "https://en.wikipedia.org/wiki/Hotellet",
			Abstract: "Wikipedia: Hotellet (Danish original title: The Hotel) is a Danish television series that originally aired on Danish channel TV 2 between 2000–2002.",
		},
		Extract: "",
		ID:      "2",
	},
	{
		DocumentBase: models.DocumentBase{
			Title:    "Wikipedia: Rosa (barge)",
			URL:      "https://en.wikipedia.org/wiki/Rosa_(barge)",
			Abstract: "Wikipedia: Rosa is a French hotel barge of Dutch origin. Since 1990 she has been offering cruises to international tourists on the Canal de Garonne in the Nouvelle Aquitaine region of South West France.",
		},
		Extract: "",
		ID:      "3",
	}}

func TestTrigramTrieInsertAndSearch(t *testing.T) {
	log := slog.New(
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
	)
	trigramTrie := New()

	ftsService := fts.NewSearchService(
		trigramTrie,
		TrigramKeys,
	)
	storage, err := leveldb.NewStorage(log, "../../../storage/fts-trie_test.db")
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	defer storage.Close()

	jobCh := make(chan models.Document)
	var wg sync.WaitGroup

	for range documents {
		wg.Go(func() {
			storage.BatchDocument(context.Background(), jobCh)
		})
	}

	for _, document := range documents {
		indexErr := ftsService.IndexDocument(
			context.Background(),
			document.ID,
			document.Abstract,
		)
		if indexErr != nil {
			t.Log("failed to index document", "error", indexErr)
		}
		fmt.Printf("Indexed document with id: %s\n", document.ID)
		jobCh <- document
		fmt.Printf("Added document with ID: %s to job channel\n", document.ID)
	}

	close(jobCh)
	wg.Wait()

	tests := []struct {
		trigram      string
		expectedDocs []fts.Document
	}{
		{
			trigram: "hot", // trigram from "hotel"
			expectedDocs: []fts.Document{
				{"1", 2},
				{"2", 2},
				{"3", 1},
			},
		},
		{
			trigram: "wik", // trigram from "wikipedia"
			expectedDocs: []fts.Document{
				{"1", 1},
				{"2", 1},
				{"3", 1},
			},
		},
		{
			trigram: "ros", // trigram from "rosa"
			expectedDocs: []fts.Document{
				{"3", 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.trigram, func(t *testing.T) {
			docs, err := trigramTrie.Search(tt.trigram)
			if err != nil {
				t.Errorf("Search error: %s", err)
			}
			if !reflect.DeepEqual(docs, tt.expectedDocs) {
				t.Errorf("Expected %v, but got %v", tt.expectedDocs, docs)
			}
		})
	}
}

func TestTrigramTrieInsertAndSearchDocument(t *testing.T) {
	log := slog.New(
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
	)
	trigramTrie := New()

	ftsService := fts.NewSearchService(
		trigramTrie,
		TrigramKeys,
	)
	storage, err := leveldb.NewStorage(log, "../../../storage/fts-trie_test.db")
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	defer storage.Close()

	jobCh := make(chan models.Document)
	var wg sync.WaitGroup

	for range documents {
		wg.Go(func() {
			storage.BatchDocument(context.Background(), jobCh)
		})
	}

	for _, document := range documents {
		indexErr := ftsService.IndexDocument(
			context.Background(),
			document.ID,
			document.Abstract,
		)
		if indexErr != nil {
			t.Log("failed to index document", "error", indexErr)
		}
		fmt.Printf("Indexed document with id: %s\n", document.ID)
		jobCh <- document
		fmt.Printf("Added document with ID: %s to job channel\n", document.ID)
	}

	close(jobCh)
	wg.Wait()

	tests := []struct {
		query               string
		expectedDocAbstract []string
	}{
		{
			query:               "hotel",
			expectedDocAbstract: []string{documents[1].Abstract, documents[0].Abstract, documents[2].Abstract},
		},
		{
			query:               "Wikipedia Hotellet",
			expectedDocAbstract: []string{documents[1].Abstract, documents[0].Abstract, documents[2].Abstract},
		},
		{
			query:               "Rosa",
			expectedDocAbstract: []string{documents[2].Abstract},
		},
	}

	for _, tt := range tests {
		fmt.Println("Start searching:", tt.query)
		t.Run(tt.query, func(t *testing.T) {
			docResults, searchErr :=
				ftsService.SearchDocuments(
					context.Background(),
					tt.query,
					10)
			if searchErr != nil {
				t.Errorf("Search error: %s", searchErr)
			}
			docs := make([]string, 0, len(docResults.ResultData))
			for _, doc := range docResults.ResultData {
				docResult, getDocErr := storage.GetDocument(context.Background(), doc.ID)
				if getDocErr != nil {
					t.Errorf("Failed to get document abstract: %v", err)
				}
				docs = append(docs, docResult.Abstract)
			}
			if !reflect.DeepEqual(docs, tt.expectedDocAbstract) {
				t.Errorf("Expected %v, but got %v", tt.expectedDocAbstract, docs)
			}
		})
	}
}
