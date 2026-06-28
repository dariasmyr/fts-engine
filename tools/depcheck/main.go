package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

const (
	rootModule       = "github.com/dariasmyr/fts-engine"
	demoModule       = "github.com/dariasmyr/fts-engine/demo"
	benchmarksModule = "github.com/dariasmyr/fts-engine/benchmarks"
)

type listedPackage struct {
	ImportPath string
	Imports    []string
	Standard   bool
	Error      *struct {
		Err string
	}
}

type importRule struct {
	ownerPrefix      string
	ownerDescription string
	allowedPrefixes  []string
	rule             string
	hint             string
}

type violation struct {
	from   string
	to     string
	rule   string
	hint   string
	detail string
}

var importRules = []importRule{
	{
		ownerPrefix:      rootModule + "/pkg/",
		ownerDescription: "pkg/*",
		allowedPrefixes:  []string{rootModule + "/pkg/"},
		rule:             "pkg/* may import only pkg/* within the repository",
		hint:             "keep library implementation inside pkg/* or move app-specific code into demo/ or benchmarks/",
	},
	{
		ownerPrefix:      rootModule + "/examples/",
		ownerDescription: "examples/*",
		allowedPrefixes:  []string{rootModule + "/pkg/"},
		rule:             "examples/* may import only public pkg/* from the repository",
		hint:             "rewrite the example to use the public library surface only",
	},
	{
		ownerPrefix:      demoModule,
		ownerDescription: "demo/*",
		allowedPrefixes:  []string{rootModule + "/pkg/", demoModule + "/internal/"},
		rule:             "demo/* may import only public pkg/* and demo/internal/*",
		hint:             "move the dependency into demo/internal/* or consume a public pkg/* package instead",
	},
	{
		ownerPrefix:      benchmarksModule,
		ownerDescription: "benchmarks/*",
		allowedPrefixes:  []string{rootModule + "/pkg/", benchmarksModule + "/internal/", benchmarksModule + "/adapters/"},
		rule:             "benchmarks/* may import only public pkg/* and benchmark-owned packages",
		hint:             "keep benchmark-only implementation inside benchmarks/internal/* or benchmarks/adapters/*",
	},
}

func main() {
	rootDir, err := os.Getwd()
	if err != nil {
		fatalf("resolve working directory: %v", err)
	}

	var violations []violation

	packagesByModule := map[string][]listedPackage{}
	for _, moduleDir := range []string{".", "demo", "benchmarks"} {
		pkgs, err := listPackages(rootDir, moduleDir)
		if err != nil {
			fatalf("list packages for %s: %v", moduleDir, err)
		}
		packagesByModule[moduleDir] = pkgs
	}

	violations = append(violations, checkImportRules(packagesByModule["."], importRules[0])...)
	violations = append(violations, checkImportRules(packagesByModule["."], importRules[1])...)
	violations = append(violations, checkImportRules(packagesByModule["demo"], importRules[2])...)
	violations = append(violations, checkImportRules(packagesByModule["benchmarks"], importRules[3])...)

	if len(violations) == 0 {
		fmt.Println("depcheck: ok")
		return
	}

	sort.Slice(violations, func(i, j int) bool {
		if violations[i].from != violations[j].from {
			return violations[i].from < violations[j].from
		}
		if violations[i].to != violations[j].to {
			return violations[i].to < violations[j].to
		}
		return violations[i].rule < violations[j].rule
	})

	for _, v := range violations {
		fmt.Fprintf(os.Stderr, "%s imports %s: %s", v.from, v.to, v.rule)
		if v.detail != "" {
			fmt.Fprintf(os.Stderr, " (%s)", v.detail)
		}
		if v.hint != "" {
			fmt.Fprintf(os.Stderr, ". %s", v.hint)
		}
		fmt.Fprintln(os.Stderr)
	}
	os.Exit(1)
}
func checkImportRules(pkgs []listedPackage, rule importRule) []violation {
	var violations []violation
	for _, pkg := range pkgs {
		if !strings.HasPrefix(pkg.ImportPath, rule.ownerPrefix) {
			continue
		}
		for _, imp := range pkg.Imports {
			if allowedImport(imp, rule.allowedPrefixes) {
				continue
			}
			if strings.HasPrefix(imp, rootModule) {
				violations = append(violations, violation{
					from:   pkg.ImportPath,
					to:     imp,
					rule:   rule.rule,
					hint:   rule.hint,
					detail: fmt.Sprintf("%s must stay within its allowed dependency boundary", rule.ownerDescription),
				})
			}
		}
	}
	return violations
}

func allowedImport(imp string, allowedPrefixes []string) bool {
	if imp == "C" || !strings.HasPrefix(imp, rootModule) {
		return true
	}
	for _, allowedPrefix := range allowedPrefixes {
		if strings.HasPrefix(imp, allowedPrefix) {
			return true
		}
	}
	return false
}

func listPackages(rootDir, moduleDir string) ([]listedPackage, error) {
	cmd := exec.Command("go", "list", "-json", "./...")
	cmd.Dir = filepath.Join(rootDir, moduleDir)

	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("%w: %s", err, strings.TrimSpace(string(ee.Stderr)))
		}
		return nil, err
	}

	dec := json.NewDecoder(bytes.NewReader(out))
	var pkgs []listedPackage
	for {
		var pkg listedPackage
		if err := dec.Decode(&pkg); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}
		if pkg.Standard {
			continue
		}
		if pkg.Error != nil && pkg.Error.Err != "" {
			return nil, fmt.Errorf("package %s: %s", pkg.ImportPath, pkg.Error.Err)
		}
		pkgs = append(pkgs, pkg)
	}
	return pkgs, nil
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
