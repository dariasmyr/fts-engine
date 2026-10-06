// Package semanticpersist stores coherent immutable generations and restores
// them as writable semantic services.
//
// A stored segment is a content-addressed directory containing vectors.bin and
// graph.bin. Generation state stores rows and liveness separately, so multiple
// generations can reference the same immutable stored-segment files.
//
// The filesystem trust boundary is local and cooperative: the store root and
// its mode-0700 descendants must not be modified concurrently except through
// package operations holding the store writer lock. The package's Lstat and
// Open checks detect ordinary corruption and accidental symlinks, but do not
// defend against a privileged or local actor racing filesystem replacement
// between checks.
package semanticpersist
