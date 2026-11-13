// Package semanticpersist provides generation-based persistence for
// semantic.Index.
//
// Save persists one already committed raw index generation. SaveService and
// CommitAndSaveService also persist the model-backed PipelineConfig required by
// LoadService. CommitAndSave and CommitAndSaveService prepare the pending
// revision under the Index mutation gate and publish it only after persistence.
// Loading owns no persistent filesystem resources.
//
// Save and Load are the lower-level operations for callers that manage
// semantic.Index directly. Save requires an explicit expected generation.
//
// The package persists a committed semantic index state as a sequence of
// immutable generations. A generation references immutable semantic segment
// objects and contains the logical state required to restore semantic.Index.
//
// # Storage model
//
// A persistence root has the following logical layout:
//
//	root/
//	    LOCK
//	    CURRENT
//
//	    objects/
//	        segments/
//	            seg-<hash>/
//	                vectors.bin
//	                graph.bin
//
//	    generations/
//	        00000000000000000001/
//	            manifest.bin
//	            semantic-state.bin
//
//	        00000000000000000002/
//	            manifest.bin
//	            semantic-state.bin
//
// Segment objects contain immutable search data:
//
//   - vectors.bin contains the prepared vector matrix;
//   - graph.bin contains the HNSW graph.
//
// Segment objects are content-addressed and may be reused by multiple
// generations.
//
// A generation represents one complete committed state of the semantic
// service. Its manifest references the segment objects used by that state
// and the semantic-state file.
//
// semantic-state.bin contains the logical index state required to resume
// writes after restore, including configuration, revision and allocation
// metadata, vector-to-chunk mappings, and liveness information. Service-aware
// generations additionally contain the immutable model pipeline configuration.
//
// # CURRENT and saving
//
// CURRENT identifies the generation visible to readers. It contains the
// generation ID and the hash of that generation's manifest.
//
// Saving a new generation follows this model:
//
//	semantic.Index
//	    ↓
//	committed state
//	    ↓
//	write/reuse immutable segment objects
//	    ↓
//	write semantic-state.bin
//	    ↓
//	write manifest.bin
//	    ↓
//	install immutable generation
//	    ↓
//	atomically replace CURRENT
//
// Installing a generation directory does not make that generation committed.
// The commit point is the successful replacement of CURRENT.
//
// This allows Save to leave an orphan generation after a crash without
// making partially saved state visible.
//
// # Loading
//
// Load performs the reverse operation:
//
//	CURRENT
//	    ↓
//	generation manifest
//	    ↓
//	semantic state + referenced segment objects
//	    ↓
//	semantic.Restore
//	    ↓
//	semantic.Index
//
// Normal Load follows CURRENT exactly. It does not scan for newer generations
// or automatically promote orphan generations.
//
// Recovery of an explicitly selected generation is performed separately by
// RecoverGeneration.
//
// # Concurrency
//
// CommitAndSave, Save, Load, RecoverGeneration, and Prune hold an exclusive
// filesystem lock for the duration of their filesystem operation. This
// prevents CURRENT replacement or pruning while a generation is being loaded.
//
// Writable locking is supported on Linux, macOS, and FreeBSD. On Windows and
// other platforms CommitAndSave, Save, Load, RecoverGeneration, and Prune return
// ErrLockUnsupported. This is intentionally stricter than ftspersist, which
// has a Windows LockFileEx implementation.
//
// CommitAndSaveService is the ordinary model-backed document-level write flow
// and requires no caller-side mutex. It holds the mutation gate through
// candidate preparation, storage publish, and in-memory publish.
// Concurrent mutations wait; search continues against the previous immutable
// view until persistence succeeds. Save and CommitAndSave remain the low-level
// operations for raw indexes and external encoder pipelines.
//
// If optimistic generation matching or filesystem persistence fails before
// CURRENT replacement, the candidate is not searchable and its mutations remain
// pending for retry. An error after CURRENT replacement may be ErrIndeterminate;
// the matching candidate is made searchable because CURRENT may already
// reference it, and a subsequent Load determines which generation is current.
//
// Prune acquires the same exclusive filesystem lock. It retains CURRENT and
// the requested number of valid committed generations, then removes only
// recognized unreachable storage entries. Unknown entries and symlinks are
// left untouched.
//
// # Successful writes
//
// CommitAndSave, Save, RecoverGeneration, and Prune fsync changed files and
// affected directories before they return successfully.
//
// # Integrity
//
// Persisted files are validated using format-specific checksums and file
// references containing file size and SHA-256 hashes.
//
// The package also validates file and directory paths before opening persisted
// objects and rejects unsupported versions, malformed data, invalid references,
// symlinks where they are not allowed, and configured resource-limit violations.
// Checksums provide accidental-corruption detection and integrity between
// referenced files; they do not authenticate a root controlled by an attacker.
// Callers must therefore protect the filesystem root from unauthorized or
// uncoordinated writers.
package semanticpersist
