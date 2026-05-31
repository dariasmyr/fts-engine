package config

import "testing"

func TestValidateConfigRejectsSnapshotMmap(t *testing.T) {
	cfg := defaultConfig()
	cfg.FTS.Persistence.Format = "snapshot"
	cfg.FTS.Persistence.Access = "mmap"

	defer func() {
		if recover() == nil {
			t.Fatal("validateConfig() panic = nil, want panic for snapshot+mmap")
		}
	}()

	validateConfig(&cfg)
}

func TestValidateConfigAllowsSegmentMmap(t *testing.T) {
	cfg := defaultConfig()
	cfg.FTS.Persistence.Format = "segment"
	cfg.FTS.Persistence.Access = "mmap"

	validateConfig(&cfg)

	if got, want := cfg.FTS.Persistence.Format, "segment"; got != want {
		t.Fatalf("Persistence.Format = %q, want %q", got, want)
	}
	if got, want := cfg.FTS.Persistence.Access, "mmap"; got != want {
		t.Fatalf("Persistence.Access = %q, want %q", got, want)
	}
}
