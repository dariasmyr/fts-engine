// Package service coordinates semantic input encoding and index operations.
//
// New constructs the standard immutable model-backed pipeline. NewService binds
// caller-owned document and query encoders to an existing semantic.Index.
// Request-local search budgets remain semantic.QueryConfig values and do not
// modify the stored index or pipeline configuration.
package service
