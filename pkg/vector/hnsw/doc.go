// Package hnsw implements immutable in-memory HNSW indexes over prepared vector
// stores. It owns construction, topology, search, neighbor selection, and
// reusable workspaces. Shared vector mathematics and contracts live in
// pkg/vector.
package hnsw
