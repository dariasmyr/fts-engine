package fts_trie

import (
	"context"
	"fmt"
	"fts-hw/internal/domain/models"
	"fts-hw/internal/storage/leveldb"
	"log/slog"
	"os"
	"reflect"
	"testing"
)

func TestInsertAndSearch(t *testing.T) {
	logger := slog.New(
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
	)
	trie := NewNode(logger)
	storage, err := leveldb.NewStorage("../../../storage/fts-trie.db")
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	defer storage.Close()

	documents := make([]models.Document, 0, 3)

	document1 := models.Document{
		DocumentBase: models.DocumentBase{
			Title:    "Wikipedia: Sans Souci Hotel (Ballston Spa)",
			URL:      "https://en.wikipedia.org/wiki/Sans_Souci_Hotel_(Ballston_Spa)",
			Abstract: "Wikipedia: The Sans Souci Hotel was a hotel located in Ballston Spa, Saratoga County, New York. It was built in 1803, closed as a hotel in 1849, and the building, used for other purposes, was torn down in 1887.",
		},
		Extract: "",
		ID:      "1",
	}

	document2 := models.Document{
		DocumentBase: models.DocumentBase{
			Title:    "Wikipedia: Hotellet",
			URL:      "https://en.wikipedia.org/wiki/Hotellet",
			Abstract: "Wikipedia: Hotellet (Danish original title: The Hotel) is a Danish television series that originally aired on Danish channel TV 2 between 2000–2002.",
		},
		Extract: "",
		ID:      "2",
	}

	document3 := models.Document{
		DocumentBase: models.DocumentBase{
			Title:    "Wikipedia: Rosa (barge)",
			URL:      "https://en.wikipedia.org/wiki/Rosa_(barge)",
			Abstract: "Wikipedia: Rosa is a French hotel barge of Dutch origin. Since 1990 she has been offering cruises to international tourists on the Canal de Garonne in the Nouvelle Aquitaine region of South West France.",
		},
		Extract: "",
		ID:      "3",
	}

	documents = append(documents, document1, document2, document3)

	for _, document := range documents {
		trie.IndexDocument(document.ID, document.Abstract)
		fmt.Printf("Indexed document with id: %s\n", document.ID)
		id, err := storage.SaveDocument(context.Background(), &document)
		if err != nil {
			t.Fatalf("Failed to add document: %v", err)
		}
		fmt.Printf("Saved document with ID: %s\n", id)
	}

	tests := []struct {
		trigram      string
		expectedDocs map[string]int
	}{
		{
			trigram: "hot", // trigram from "hotel"
			expectedDocs: map[string]int{
				document1.ID: 3,
				document2.ID: 2,
				document3.ID: 1,
			},
		},
		{
			trigram: "wik", // trigram from "wikipedia"
			expectedDocs: map[string]int{
				document1.ID: 1,
				document2.ID: 1,
				document3.ID: 1,
			},
		},
		{
			trigram: "ros", // trigram from "rosa"
			expectedDocs: map[string]int{
				document3.ID: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.trigram, func(t *testing.T) {
			docs, err := trie.Search(tt.trigram)
			if err != nil {
				t.Errorf("Search error: %s", err)
			}
			if !reflect.DeepEqual(docs, tt.expectedDocs) {
				t.Errorf("Expected %v, but got %v", tt.expectedDocs, docs)
			}
		})
	}
}

func TestInsertAndSearchDocument(t *testing.T) {
	logger := slog.New(
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
	)
	trie := NewNode(logger)
	storage, err := leveldb.NewStorage("../../../storage/fts-trie.db")
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	defer storage.Close()

	documents := make([]models.Document, 0, 3)

	document1 := models.Document{
		DocumentBase: models.DocumentBase{
			Title:    "Wikipedia: Sans Souci Hotel (Ballston Spa)",
			URL:      "https://en.wikipedia.org/wiki/Sans_Souci_Hotel_(Ballston_Spa)",
			Abstract: "Wikipedia: The Sans Souci Hotel was a hotel located in Ballston Spa, Saratoga County, New York. It was built in 1803, closed as a hotel in 1849, and the building, used for other purposes, was torn down in 1887.",
		},
		Extract: "",
		ID:      "1",
	}

	document2 := models.Document{
		DocumentBase: models.DocumentBase{
			Title:    "Wikipedia: Hotellet",
			URL:      "https://en.wikipedia.org/wiki/Hotellet",
			Abstract: "Wikipedia: Hotellet (Danish original title: The Hotel) is a Danish television series that originally aired on Danish channel TV 2 between 2000–2002.",
		},
		Extract: "",
		ID:      "2",
	}

	document3 := models.Document{
		DocumentBase: models.DocumentBase{
			Title:    "Wikipedia: Rosa (barge)",
			URL:      "https://en.wikipedia.org/wiki/Rosa_(barge)",
			Abstract: "Wikipedia: Rosa is a French hotel barge of Dutch origin. Since 1990 she has been offering cruises to international tourists on the Canal de Garonne in the Nouvelle Aquitaine region of South West France.",
		},
		Extract: "",
		ID:      "3",
	}

	documents = append(documents, document1, document2, document3)

	for _, document := range documents {
		trie.IndexDocument(document.ID, document.Abstract)
		fmt.Printf("Indexed document with id: %s\n", document.ID)
		id, err := storage.SaveDocument(context.Background(), &document)
		if err != nil {
			t.Fatalf("Failed to add document: %v", err)
		}
		fmt.Printf("Saved document with ID: %s\n", id)
	}

	tests := []struct {
		query               string
		expectedDocAbstract []string
	}{
		{
			query:               "hotel",
			expectedDocAbstract: []string{document1.Abstract, document2.Abstract, document3.Abstract},
		},
		{
			query:               "Wikipedia",
			expectedDocAbstract: []string{document1.Abstract, document2.Abstract, document3.Abstract},
		},
		{
			query:               "Rosa",
			expectedDocAbstract: []string{document3.Abstract},
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			docResults, err := trie.SearchDocuments(context.Background(), tt.query, 10)
			if err != nil {
				t.Errorf("Search error: %s", err)
			}
			docs := make([]string, 0, len(docResults.ResultData))
			for _, doc := range docResults.ResultData {
				docResult, err := storage.GetDocument(context.Background(), doc.ID)
				if err != nil {
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
