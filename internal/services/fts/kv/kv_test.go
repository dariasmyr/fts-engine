package kv

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

func TestInsertAndSearchDocument(t *testing.T) {
	log := slog.New(
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
	)
	storage, err := leveldb.NewStorage(log, "../../../storage/fts-kv.db")
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	defer storage.Close()

	keyValueFTS := New(log, storage, storage)

	for _, document := range documents {
		indexErr := keyValueFTS.IndexDocument(
			context.Background(),
			document.ID,
			document.Abstract,
		)
		if indexErr != nil {
			t.Errorf("Failed to process document: %v", indexErr)
		}
		_, err = storage.SaveDocument(context.Background(), document)
		if err != nil {
			t.Errorf("Failed to save document: %v", err)
		}
		fmt.Println("Saved document")
	}

	tests := []struct {
		query               string
		expectedDocAbstract []string
	}{
		{
			query:               "hotel",
			expectedDocAbstract: []string{documents[0].Abstract, documents[1].Abstract, documents[2].Abstract},
		},
		{
			query:               "Wikipedia",
			expectedDocAbstract: []string{documents[0].Abstract, documents[1].Abstract, documents[2].Abstract},
		},
		{
			query:               "Rosa",
			expectedDocAbstract: []string{documents[2].Abstract},
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			docResults, searchErr := keyValueFTS.SearchDocuments(context.Background(), tt.query, 10)
			if searchErr != nil {
				t.Errorf("Search error: %s", searchErr)
			}
			docs := make([]string, 0, len(docResults.ResultData))
			for _, resultInfo := range docResults.ResultData {
				fmt.Printf("resultInfo %+v \n", resultInfo)
				docResult, err := storage.GetDocument(context.Background(), resultInfo.ID)
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
