package fts

import (
	"strings"
	"testing"
)

func TestHighlightTermMatch(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys)

	text := "james doe gave a long speech in the rose garden today."
	frags := svc.Highlight("doe", text, Highlighter{})
	if len(frags) != 1 {
		t.Fatalf("frags = %d, want 1", len(frags))
	}
	if !strings.Contains(frags[0].Text, "<mark>doe</mark>") {
		t.Fatalf("expected wrapped 'doe', got %q", frags[0].Text)
	}
	if frags[0].Matches != 1 {
		t.Fatalf("Matches = %d, want 1", frags[0].Matches)
	}
}

func TestHighlightMultipleMatches(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys)

	text := "The doe foundation was founded by doe himself."
	frags := svc.Highlight("doe", text, Highlighter{})
	if len(frags) != 1 {
		t.Fatalf("frags = %d, want 1 (clustered)", len(frags))
	}
	if got := strings.Count(frags[0].Text, "<mark>"); got != 2 {
		t.Fatalf("mark count = %d, want 2 (got %q)", got, frags[0].Text)
	}
	if frags[0].Matches != 2 {
		t.Fatalf("Matches = %d, want 2", frags[0].Matches)
	}
}

func TestHighlightSplitsDistantMatches(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys)

	prefix := "doe. " + strings.Repeat("filler word ", 40)
	text := prefix + "doe again."

	frags := svc.Highlight("doe", text, Highlighter{FragmentSize: 50})
	if len(frags) < 2 {
		t.Fatalf("want >=2 fragments for distant matches, got %d (%q)", len(frags), frags)
	}
}

func TestHighlightCustomTags(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys)

	text := "alpha beta gamma"
	frags := svc.Highlight("beta", text, Highlighter{PreTag: "[", PostTag: "]"})
	if len(frags) != 1 || !strings.Contains(frags[0].Text, "[beta]") {
		t.Fatalf("custom tags not applied, got %+v", frags)
	}
}

func TestHighlightNoMatch(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys)

	frags := svc.Highlight("xyz", "alpha beta gamma", Highlighter{})
	if len(frags) != 0 {
		t.Fatalf("want 0 frags on miss, got %+v", frags)
	}
}

func TestHighlightUsesPipelineNormalization(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys, WithPipeline(uppercasePipeline{}))

	text := "abc DEF ghi"
	frags := svc.Highlight("abc", text, Highlighter{})
	if len(frags) != 1 {
		t.Fatalf("frags = %+v, want 1", frags)
	}
	if !strings.Contains(frags[0].Text, "<mark>abc</mark>") {
		t.Fatalf("expected pipeline-normalized match, got %q", frags[0].Text)
	}
}

func TestHighlightRespectsMaxFragments(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys)

	var sb strings.Builder
	for i := 0; i < 10; i++ {
		sb.WriteString("doe ")
		sb.WriteString(strings.Repeat("filler ", 30))
	}
	frags := svc.Highlight("doe", sb.String(), Highlighter{FragmentSize: 50, MaxFragments: 2})
	if len(frags) != 2 {
		t.Fatalf("want 2 fragments (capped), got %d", len(frags))
	}
}

func TestHighlightSurvivesUnicode(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys)

	text := "Это большой пример текста про обамка и его речь."
	frags := svc.Highlight("обамка", text, Highlighter{})
	if len(frags) != 1 || !strings.Contains(frags[0].Text, "<mark>обамка</mark>") {
		t.Fatalf("Unicode highlight failed: %+v", frags)
	}
}

func TestHighlightIgnoresFieldNamesInQuery(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys)

	frags := svc.Highlight("title:beta", "title beta gamma", Highlighter{})
	if len(frags) != 1 {
		t.Fatalf("frags = %d, want 1", len(frags))
	}
	if strings.Contains(frags[0].Text, "<mark>title</mark>") {
		t.Fatalf("field name should not be highlighted: %q", frags[0].Text)
	}
	if !strings.Contains(frags[0].Text, "<mark>beta</mark>") {
		t.Fatalf("term should be highlighted: %q", frags[0].Text)
	}
}

func TestHighlightSkipsMustNotClauses(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys)

	frags := svc.Highlight("beta -gamma", "beta gamma", Highlighter{})
	if len(frags) != 1 {
		t.Fatalf("frags = %d, want 1", len(frags))
	}
	if strings.Contains(frags[0].Text, "<mark>gamma</mark>") {
		t.Fatalf("must-not term should not be highlighted: %q", frags[0].Text)
	}
	if !strings.Contains(frags[0].Text, "<mark>beta</mark>") {
		t.Fatalf("positive term should be highlighted: %q", frags[0].Text)
	}
}

func TestHighlightSupportsPrefixQueries(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys)

	frags := svc.Highlight("doe*", "doe doecare orbit", Highlighter{})
	if len(frags) != 1 {
		t.Fatalf("frags = %d, want 1", len(frags))
	}
	if got := strings.Count(frags[0].Text, "<mark>"); got != 2 {
		t.Fatalf("mark count = %d, want 2 (got %q)", got, frags[0].Text)
	}
}

func TestHighlightPlainTextTreatsFieldLikeTextAsPlainTokens(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys)

	frags := svc.HighlightPlainText("title:beta", "title beta gamma", Highlighter{})
	if len(frags) != 1 {
		t.Fatalf("frags = %d, want 1", len(frags))
	}
	if !strings.Contains(frags[0].Text, "<mark>title</mark>") {
		t.Fatalf("plain-text highlight should include title token: %q", frags[0].Text)
	}
	if !strings.Contains(frags[0].Text, "<mark>beta</mark>") {
		t.Fatalf("plain-text highlight should include beta token: %q", frags[0].Text)
	}
}
