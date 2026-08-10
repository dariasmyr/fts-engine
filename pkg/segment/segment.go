// Package segment defines a flat-file format for sealed FTS indexes.
// Sealed segments are immutable: built once from an in-memory index, then
// queried by holding the file bytes plus a small term lookup table.
package segment

const (
	magic         = "FTSE"
	legacyVersion = uint16(1)
	version       = uint16(2)

	headerLen       = 8
	legacyFooterLen = 24
	footerLen       = 32
)
