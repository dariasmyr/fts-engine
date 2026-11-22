// Package semantic implements a mutable document-level semantic index over
// chunk embeddings. Mutations are staged until Commit publishes an immutable
// IndexView; search runs only against committed views. ExportState returns a
// CommittedIndexState and rejects pending mutations. CommitAndExportState makes
// the pending-to-committed transition and exports that exact revision under one
// mutation gate. CommitWithState lets an external operation accept a prepared
// state before its matching view becomes searchable. Physical ANN storage is
// segmented and may be compacted without changing semantic identities. Index
// operations are in-memory only; package semanticpersist provides durable
// filesystem persistence. Document and query encoding live in semantic/encoding,
// while semantic/service coordinates encoders with an Index.
package semantic
