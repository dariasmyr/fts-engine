// Package semantic implements a mutable document-level semantic index over
// chunk embeddings. Mutations are staged until Flush publishes an immutable
// Snapshot; search runs only against committed snapshots. Physical ANN storage
// is segmented and may be compacted without changing semantic identities.
package semantic
