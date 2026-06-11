package dataset

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadMSMARCOLimitsQueriesAndPreservesMustHaveDocs(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "collection.tsv"), "d1\talpha one\nd2\tbeta two\nd3\tgamma three\nd4\tdelta four\n")
	writeFile(t, filepath.Join(dir, "queries.dev.small.tsv"), "q1\talpha beta\nq2\tgamma\nq3\tmissing\n")
	writeFile(t, filepath.Join(dir, "qrels.dev.small.tsv"), "q1\t0\td1\t1\nq1\t0\td2\t1\nq2\t0\td3\t1\nq3\t0\td9\t1\n")

	corpus, err := LoadMSMARCO(MSMARCOConfig{Dir: dir, MaxDocs: 2, MaxQueries: 2, K: 10, Seed: 7})
	if err != nil {
		t.Fatalf("LoadMSMARCO() error = %v", err)
	}
	if len(corpus.Queries) != 2 {
		t.Fatalf("len(Queries) = %d, want 2", len(corpus.Queries))
	}
	if len(corpus.Docs) != 3 {
		t.Fatalf("len(Docs) = %d, want 3 because qrel positives are preserved even when they exceed max-docs", len(corpus.Docs))
	}
	docSet := make(map[string]struct{}, len(corpus.Docs))
	for _, doc := range corpus.Docs {
		docSet[doc.ID] = struct{}{}
	}
	for _, id := range []string{"d1", "d2", "d3"} {
		if _, ok := docSet[id]; !ok {
			t.Fatalf("must-have doc %q missing from sampled corpus", id)
		}
	}
	if _, ok := corpus.Qrels["q1"]["d1"]; !ok {
		t.Fatal("q1 relevant doc d1 missing after filtering")
	}
	if _, ok := corpus.Qrels["q2"]["d3"]; !ok {
		t.Fatal("q2 relevant doc d3 missing after filtering")
	}
}

func TestLoadMSMARCODropsQueriesWithoutRemainingRelevantDocs(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "collection.tsv"), "d1\talpha one\nd2\tbeta two\n")
	writeFile(t, filepath.Join(dir, "queries.dev.small.tsv"), "q1\talpha\nq2\tghost\n")
	writeFile(t, filepath.Join(dir, "qrels.dev.small.tsv"), "q1\t0\td1\t1\nq2\t0\td9\t1\n")

	corpus, err := LoadMSMARCO(MSMARCOConfig{Dir: dir, MaxDocs: 1, MaxQueries: 0, K: 10, Seed: 1})
	if err != nil {
		t.Fatalf("LoadMSMARCO() error = %v", err)
	}
	if len(corpus.Queries) != 1 {
		t.Fatalf("len(Queries) = %d, want 1", len(corpus.Queries))
	}
	if corpus.Queries[0].ID != "q1" {
		t.Fatalf("Queries[0].ID = %q, want q1", corpus.Queries[0].ID)
	}
	if _, ok := corpus.Qrels["q2"]; ok {
		t.Fatal("q2 should have been dropped because no relevant docs remain in corpus")
	}
}

func writeFile(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
}
