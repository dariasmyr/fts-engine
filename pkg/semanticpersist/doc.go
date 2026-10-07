// Package semanticpersist provides durable persistence for semantic.Service.
//
// The package persists a committed semantic service state as a sequence of
// immutable generations. A generation references immutable semantic segment
// objects and contains the logical state required to restore semantic.Service.
//
// # Storage model
//
// A store has the following logical layout:
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
// A generation represents one complete committed snapshot of the semantic
// service. Its manifest references the segment objects used by that snapshot
// and the semantic-state file.
//
// semantic-state.bin contains the logical service state required to resume
// writes after restore, including configuration, revision and allocation
// metadata, vector-to-chunk mappings, and liveness information.
//
// # CURRENT and publication
//
// CURRENT identifies the generation visible to readers. It contains the
// generation ID and the hash of that generation's manifest.
//
// Publishing a new generation follows this model:
//
//	semantic.Service
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
// This allows publication to leave an orphan generation after a crash without
// making partially published state visible.
//
// # Opening a store
//
// Opening performs the reverse operation:
//
//	CURRENT
//	    ↓
//	generation manifest
//	    ↓
//	semantic state + referenced segment objects
//	    ↓
//	semantic.Restore
//	    ↓
//	semantic.Service
//
// Normal Open follows CURRENT exactly. It does not scan for newer generations
// or automatically promote orphan generations.
//
// Recovery of an explicitly selected generation is performed separately by
// OpenByGeneration.
//
// # Concurrency
//
// A writable store owns an exclusive filesystem lock for its lifetime.
// This prevents multiple processes from concurrently publishing generations
// based on the same CURRENT state.
//
// Publication through an opened Store is additionally serialized within the
// process.
//
// # Durability
//
// Synchronous durability fsyncs published files and affected directories
// before publication is considered durable.
//
// Asynchronous durability preserves atomic visibility of generations but does
// not guarantee that the latest committed generation survives sudden power
// loss.
//
// # Integrity
//
// Persisted files are validated using format-specific checksums and file
// references containing file size and SHA-256 hashes.
//
// The package also validates file and directory paths before opening persisted
// objects and rejects unsupported versions, malformed data, invalid references,
// symlinks where they are not allowed, and configured resource-limit violations.
package semanticpersist
