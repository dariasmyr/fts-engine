package posting

// TermData is the storage-neutral export representation of one term.
type TermData struct {
	Term     string
	Postings []Posting
	// Positions[i] contains sorted positions for Postings[i]. Positions may
	// be shorter than Postings; a missing entry means no stored positions.
	Positions [][]uint32
}

// Exporter emits immutable term data in strictly increasing raw term order for
// a sealed storage writer. Each term is emitted once. Slices remain owned by the
// exporter, must not be mutated, and are only valid for the callback duration.
type Exporter interface {
	ExportPostings(func(TermData) error) error
}
