// Package format owns the SSTA, SMAN, and SCUR binary wire formats for the
// owning semantic persistence package.
package format

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"unicode/utf8"

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

type encoder struct {
	buffer bytes.Buffer
	limit  uint64
	err    error
}

func newEncoder(magic string, version uint16, limits Limits) *encoder {
	e := &encoder{limit: limits.MaxFileBytes}
	e.raw([]byte(magic))
	e.u16(version)
	e.u16(0)
	return e
}

func (e *encoder) raw(value []byte) {
	if e.err != nil {
		return
	}
	if uint64(len(value)) > e.limit-uint64(e.buffer.Len()) {
		e.err = ErrLimitExceeded
		return
	}
	_, e.err = e.buffer.Write(value)
}

func (e *encoder) u8(value uint8) { e.raw([]byte{value}) }

func (e *encoder) u16(value uint16) {
	var data [2]byte
	binary.LittleEndian.PutUint16(data[:], value)
	e.raw(data[:])
}

func (e *encoder) u32(value uint32) {
	var data [4]byte
	binary.LittleEndian.PutUint32(data[:], value)
	e.raw(data[:])
}

func (e *encoder) u64(value uint64) {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], value)
	e.raw(data[:])
}

func (e *encoder) string(value string, max int) {
	if !utf8.ValidString(value) || len(value) > max || uint64(len(value)) > math.MaxUint32 {
		e.err = ErrLimitExceeded
		return
	}
	e.u32(uint32(len(value)))
	e.raw([]byte(value))
}

func (e *encoder) finish() ([]byte, FileReference, error) {
	if e.err != nil {
		return nil, FileReference{}, e.err
	}
	data := internalformat.AppendCRC32(e.buffer.Bytes())
	return data, FileReference{Size: uint64(len(data)), SHA256: sha256.Sum256(data)}, nil
}

type decoder struct {
	data           []byte
	offset         int
	maxStringBytes int
	decodeErr      error
}

func newDecoder(data []byte, magic string, version uint16, limits Limits) (*decoder, error) {
	if len(data) < 8+internalformat.CRC32Size {
		return nil, ErrCorrupt
	}
	body := data[:len(data)-internalformat.CRC32Size]
	if string(body[:4]) != magic {
		return nil, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(body[4:6]) != version {
		return nil, ErrUnsupportedVersion
	}
	if binary.LittleEndian.Uint16(body[6:8]) != 0 {
		return nil, ErrCorrupt
	}
	checksum, ok := internalformat.ReadCRC32(data[len(data)-internalformat.CRC32Size:])
	if !ok || internalformat.CRC32(body) != checksum {
		return nil, ErrCorrupt
	}
	return &decoder{data: body, offset: 8, maxStringBytes: limits.MaxStringBytes}, nil
}

func (d *decoder) take(size int) []byte {
	if d.decodeErr != nil || size < 0 || size > len(d.data)-d.offset {
		d.decodeErr = ErrCorrupt
		return nil
	}
	value := d.data[d.offset : d.offset+size]
	d.offset += size
	return value
}

func (d *decoder) u8() uint8 {
	data := d.take(1)
	if data == nil {
		return 0
	}
	return data[0]
}

func (d *decoder) u16() uint16 {
	data := d.take(2)
	if data == nil {
		return 0
	}
	return binary.LittleEndian.Uint16(data)
}

func (d *decoder) u32() uint32 {
	data := d.take(4)
	if data == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(data)
}

func (d *decoder) u64() uint64 {
	data := d.take(8)
	if data == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(data)
}

func (d *decoder) string() string {
	size := uint64(d.u32())
	if d.decodeErr != nil {
		return ""
	}
	if size > uint64(d.maxStringBytes) {
		d.decodeErr = ErrLimitExceeded
		return ""
	}
	if size > uint64(len(d.data)-d.offset) {
		d.decodeErr = ErrCorrupt
		return ""
	}
	value := string(d.take(int(size)))
	if !utf8.ValidString(value) {
		d.decodeErr = ErrCorrupt
		return ""
	}
	return value
}

func (d *decoder) done() error {
	if d.decodeErr != nil {
		return d.decodeErr
	}
	if d.offset != len(d.data) {
		return ErrCorrupt
	}
	return nil
}

func (d *decoder) remaining() int {
	if d.decodeErr != nil {
		return 0
	}
	return len(d.data) - d.offset
}

func (d *decoder) err() error { return d.decodeErr }
