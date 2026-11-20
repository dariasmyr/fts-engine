// Package ftspersist provides filesystem persistence for FTS snapshots and
// segments.
//
// Advisory generation leases use flock on Unix and LockFileEx on Windows. On
// other platforms lock files are opened without an operating-system advisory
// lock, preserving the package's historical best-effort behavior.
package ftspersist
