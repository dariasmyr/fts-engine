// Package format owns the SSTA, SMAN, and SCUR binary wire formats for the
// owning semantic persistence package.
package format

import (
	"crypto/sha256"

	internalformat "github.com/dariasmyr/fts-engine/internal/format"
)

var (
	ErrCorrupt            = errorString("semanticpersist/format: corrupt data")
	ErrUnsupportedVersion = errorString("semanticpersist/format: unsupported version")
	ErrLimitExceeded      = errorString("semanticpersist/format: configured limit exceeded")
)

type errorString string

func (e errorString) Error() string { return string(e) }

// Limits bounds semantic persistence codec input and output.
type Limits struct {
	MaxFileBytes         uint64
	MaxDimensions        int
	MaxVectors           int
	MaxDocuments         int
	MaxStringBytes       int
	MaxChunksPerDocument int
	MaxK                 int
}

// FileReference identifies an immutable file by size and SHA-256.
type FileReference struct {
	Size   uint64
	SHA256 [sha256.Size]byte
}

type encoder struct{ codec *internalformat.Encoder }

func newEncoder(magic string, version uint16, limits Limits) *encoder {
	return &encoder{codec: internalformat.NewEncoder(magic, version, limits.MaxFileBytes, internalformat.Errors{
		LimitExceeded: ErrLimitExceeded, Corrupt: ErrCorrupt, UnsupportedVersion: ErrUnsupportedVersion,
	})}
}

func (e *encoder) raw(value []byte)             { e.codec.Raw(value) }
func (e *encoder) u8(value uint8)               { e.codec.U8(value) }
func (e *encoder) u16(value uint16)             { e.codec.U16(value) }
func (e *encoder) u32(value uint32)             { e.codec.U32(value) }
func (e *encoder) u64(value uint64)             { e.codec.U64(value) }
func (e *encoder) string(value string, max int) { e.codec.String(value, max) }

func (e *encoder) finish() ([]byte, FileReference, error) {
	data, err := e.codec.Finish()
	if err != nil {
		return nil, FileReference{}, err
	}
	data = internalformat.AppendCRC32(data)
	return data, FileReference{Size: uint64(len(data)), SHA256: sha256.Sum256(data)}, nil
}

type decoder struct{ codec *internalformat.Decoder }

func newDecoder(data []byte, magic string, version uint16, limits Limits) (*decoder, error) {
	if len(data) < 12 {
		return nil, ErrCorrupt
	}
	body := data[:len(data)-internalformat.CRC32Size]
	codec, err := internalformat.NewDecoder(body, magic, version, internalformat.Limits{MaxStringBytes: limits.MaxStringBytes}, internalformat.Errors{
		LimitExceeded: ErrLimitExceeded, Corrupt: ErrCorrupt, UnsupportedVersion: ErrUnsupportedVersion,
	})
	if err != nil {
		return nil, err
	}
	checksum, ok := internalformat.ReadCRC32(data[len(data)-internalformat.CRC32Size:])
	if !ok || internalformat.CRC32(body) != checksum {
		return nil, ErrCorrupt
	}
	return &decoder{codec: codec}, nil
}

func (d *decoder) take(size int) []byte { return d.codec.Take(size) }
func (d *decoder) u8() uint8            { return d.codec.U8() }
func (d *decoder) u16() uint16          { return d.codec.U16() }
func (d *decoder) u32() uint32          { return d.codec.U32() }
func (d *decoder) u64() uint64          { return d.codec.U64() }
func (d *decoder) string() string       { return d.codec.String() }
func (d *decoder) done() error          { return d.codec.Done() }
func (d *decoder) remaining() int       { return d.codec.Remaining() }
func (d *decoder) err() error           { return d.codec.Err() }
