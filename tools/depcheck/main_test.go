package main

import (
	"strings"
	"testing"
)

func TestBenchmarksModuleMatchesRootMajorVersion(t *testing.T) {
	t.Parallel()

	if benchmarksModule != rootModule+"/benchmarks" {
		t.Fatalf("benchmarksModule = %q, want %q", benchmarksModule, rootModule+"/benchmarks")
	}
}

func TestArchitectureRulesAllowIntendedDependencies(t *testing.T) {
	t.Parallel()

	packages := []listedPackage{
		{
			ImportPath: rootModule + "/pkg/embedding/ollama",
			Imports: []string{
				rootModule + "/pkg/embedding",
			},
		},
		{
			ImportPath: rootModule + "/pkg/vector/hnsw",
			Imports: []string{
				rootModule + "/internal/hnswmodel",
				rootModule + "/pkg/vector",
			},
		},
		{
			ImportPath: rootModule + "/pkg/semantic",
			Imports: []string{
				rootModule + "/internal/memorystore",
				rootModule + "/pkg/fts",
				rootModule + "/pkg/vector/hnsw",
			},
		},
		{
			ImportPath: rootModule + "/pkg/semanticpersist",
			Imports: []string{
				rootModule + "/internal/format",
				rootModule + "/internal/memorystore",
				rootModule + "/internal/persistfs",
				rootModule + "/pkg/embedding",
				rootModule + "/pkg/semantic",
				rootModule + "/pkg/semanticpersist/internal/generationformat",
				rootModule + "/pkg/semanticpersist/internal/hnswformat",
				rootModule + "/pkg/semanticpersist/internal/storage",
				rootModule + "/pkg/vector/hnsw",
			},
		},
		{
			ImportPath: rootModule + "/pkg/semanticpersist/internal/storage",
			Imports: []string{
				rootModule + "/internal/persistfs",
			},
		},
		{
			ImportPath: rootModule + "/pkg/semanticpersist/internal/generationformat",
			Imports: []string{
				"bytes",
				"io",
				rootModule + "/internal/format",
				rootModule + "/pkg/semantic",
			},
		},
		{
			ImportPath: rootModule + "/pkg/semanticpersist/internal/hnswformat",
			Imports: []string{
				"io",
				rootModule + "/internal/format",
				rootModule + "/internal/hnswmodel",
				rootModule + "/pkg/vector/hnsw",
			},
		},
	}

	if violations := checkArchitectureRules(packages, architectureRules); len(violations) != 0 {
		t.Fatalf("checkArchitectureRules() violations = %+v, want none", violations)
	}
}

func TestArchitectureRulesRejectForbiddenEdges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from string
		to   string
		rule string
	}{
		{
			name: "vector to semantic",
			from: rootModule + "/pkg/vector",
			to:   rootModule + "/pkg/semantic",
			rule: "pkg/vector",
		},
		{
			name: "hnsw to semantic persistence",
			from: rootModule + "/pkg/vector/hnsw",
			to:   rootModule + "/pkg/semanticpersist",
			rule: "pkg/vector/hnsw",
		},
		{
			name: "semantic to persistence",
			from: rootModule + "/pkg/semantic",
			to:   rootModule + "/pkg/semanticpersist",
			rule: "pkg/semantic",
		},
		{
			name: "semantic storage to public persistence",
			from: rootModule + "/pkg/semanticpersist/internal/storage",
			to:   rootModule + "/pkg/semanticpersist",
			rule: "pkg/semanticpersist/internal/storage",
		},
		{
			name: "persistence to unrelated package",
			from: rootModule + "/pkg/semanticpersist",
			to:   rootModule + "/pkg/filter",
			rule: "pkg/semanticpersist",
		},
		{
			name: "generation format to filesystem",
			from: rootModule + "/pkg/semanticpersist/internal/generationformat",
			to:   "os",
			rule: "generationformat",
		},
		{
			name: "hnsw format to publication primitives",
			from: rootModule + "/pkg/semanticpersist/internal/hnswformat",
			to:   rootModule + "/internal/persistfs",
			rule: "hnswformat",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			violations := checkArchitectureRules([]listedPackage{{
				ImportPath: tt.from,
				Imports:    []string{tt.to},
			}}, architectureRules)
			if len(violations) != 1 {
				t.Fatalf("checkArchitectureRules() returned %d violations, want 1: %+v", len(violations), violations)
			}
			if violations[0].from != tt.from || violations[0].to != tt.to || !strings.Contains(violations[0].rule, tt.rule) {
				t.Fatalf("violation = %+v, want edge %s -> %s and rule containing %q", violations[0], tt.from, tt.to, tt.rule)
			}
		})
	}
}

func TestArchitectureRulePackageMatchingUsesBoundaries(t *testing.T) {
	t.Parallel()

	if packageMatches(rootModule+"/pkg/semanticpersist", rootModule+"/pkg/semantic") {
		t.Fatal("semantic prefix matched semanticpersist sibling")
	}
	if !packageMatches(rootModule+"/pkg/vector/hnsw", rootModule+"/pkg/vector") {
		t.Fatal("vector prefix did not match vector/hnsw child")
	}
}

func TestArchitectureCoverageRequiresRulesForNewPackages(t *testing.T) {
	t.Parallel()

	packages := []listedPackage{
		{ImportPath: rootModule + "/pkg/vector"},
		{ImportPath: rootModule + "/pkg/index/flat"},
		{ImportPath: rootModule + "/internal/persistfs"},
		{ImportPath: rootModule + "/pkg/newfeature"},
		{ImportPath: rootModule + "/examples/example"},
	}
	violations := checkArchitectureCoverage(packages, architectureRules)
	if len(violations) != 1 {
		t.Fatalf("checkArchitectureCoverage() violations = %+v, want one", violations)
	}
	if violations[0].from != rootModule+"/pkg/newfeature" {
		t.Fatalf("coverage violation = %+v, want pkg/newfeature", violations[0])
	}
}
