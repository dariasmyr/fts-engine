// Package hnswformat encodes and decodes immutable HNSW segment files.
//
// It owns the coupled vector and graph formats plus conversion between the
// validated graph representation and vector/hnsw indexes. Filesystem object
// lifecycle remains the responsibility of semanticpersist.
package hnswformat
