package wiki

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func TestLoaderLoadDocumentsHonorsLimit(t *testing.T) {
	t.Parallel()

	dumpPath := writeTestDump(t, 4)
	loader := New(slog.New(slog.NewTextHandler(io.Discard, nil)), dumpPath, 2)

	documents, err := loader.LoadDocuments(context.Background())
	if err != nil {
		t.Fatalf("LoadDocuments() error = %v", err)
	}
	if len(documents) != 2 {
		t.Fatalf("documents = %d, want 2", len(documents))
	}
	if documents[0].Title != "title-0" || documents[1].Title != "title-1" {
		t.Fatalf("documents = %+v, want first two dump documents", documents)
	}
	for i, document := range documents {
		if document.ID == "" {
			t.Fatalf("document %d has empty ID", i)
		}
	}
}

func TestLoaderLoadDocumentsZeroLimitLoadsCompleteDump(t *testing.T) {
	t.Parallel()

	dumpPath := writeTestDump(t, 4)
	loader := New(slog.New(slog.NewTextHandler(io.Discard, nil)), dumpPath, 0)

	documents, err := loader.LoadDocuments(context.Background())
	if err != nil {
		t.Fatalf("LoadDocuments() error = %v", err)
	}
	if len(documents) != 4 {
		t.Fatalf("documents = %d, want 4", len(documents))
	}
}

func TestLoaderLoadDocumentsReturnsMalformedXMLError(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "malformed.xml.gz")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create dump: %v", err)
	}
	gz := gzip.NewWriter(file)
	if _, err := io.WriteString(gz, "<feed><doc><title>not closed</doc></feed>"); err != nil {
		t.Fatalf("write malformed dump: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close dump: %v", err)
	}

	loader := New(slog.New(slog.NewTextHandler(io.Discard, nil)), path, 0)
	if _, err := loader.LoadDocuments(context.Background()); err == nil {
		t.Fatal("LoadDocuments() error = nil, want malformed XML error")
	}
}

func writeTestDump(t *testing.T, documentCount int) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "wiki.xml.gz")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create dump: %v", err)
	}
	gz := gzip.NewWriter(file)
	if _, err := io.WriteString(gz, "<feed>"); err != nil {
		t.Fatalf("write dump root: %v", err)
	}
	for i := range documentCount {
		if _, err := fmt.Fprintf(gz, "<doc><title>title-%d</title><url>url-%d</url><abstract>abstract-%d</abstract></doc>", i, i, i); err != nil {
			t.Fatalf("write document: %v", err)
		}
	}
	if _, err := io.WriteString(gz, "</feed>"); err != nil {
		t.Fatalf("write dump end: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close dump: %v", err)
	}
	return path
}
