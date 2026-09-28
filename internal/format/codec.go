// Package format provides bounded binary framing primitives for internal
// persistence formats.
package format

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"unicode/utf8"
)

var (
	defaultLimitExceeded      = errors.New("format: limit exceeded")
	defaultCorrupt            = errors.New("format: corrupt data")
	defaultUnsupportedVersion = errors.New("format: unsupported version")
)

// Errors allows a caller to retain package-specific sentinel errors while
// sharing the binary framing implementation.
type Errors struct {
	LimitExceeded      error
	Corrupt            error
	UnsupportedVersion error
}

func (e Errors) withDefaults() Errors {
	if e.LimitExceeded == nil {
		e.LimitExceeded = defaultLimitExceeded
	}
	if e.Corrupt == nil {
		e.Corrupt = defaultCorrupt
	}
	if e.UnsupportedVersion == nil {
		e.UnsupportedVersion = defaultUnsupportedVersion
	}
	return e
}

// Limits bounds the encoded file and individual strings.
type Limits struct {
	MaxFileBytes   uint64
	MaxStringBytes int
}

// Encoder writes a little-endian, versioned binary payload with a caller-owned
// error vocabulary. The payload footer is added by the caller's format.
type Encoder struct {
	buffer bytes.Buffer
	limit  uint64
	errors Errors
	err    error
}

func NewEncoder(magic string, version uint16, limit uint64, errors Errors) *Encoder {
	e := &Encoder{limit: limit, errors: errors.withDefaults()}
	e.Raw([]byte(magic))
	e.U16(version)
	e.U16(0)
	return e
}

func (e *Encoder) Raw(value []byte) {
	if e.err != nil {
		return
	}
	if uint64(len(value)) > e.limit-uint64(e.buffer.Len()) {
		e.err = e.errors.LimitExceeded
		return
	}
	_, e.err = e.buffer.Write(value)
}

func (e *Encoder) U8(value uint8) { e.Raw([]byte{value}) }

func (e *Encoder) U16(value uint16) {
	var data [2]byte
	binary.LittleEndian.PutUint16(data[:], value)
	e.Raw(data[:])
}

func (e *Encoder) U32(value uint32) {
	var data [4]byte
	binary.LittleEndian.PutUint32(data[:], value)
	e.Raw(data[:])
}

func (e *Encoder) U64(value uint64) {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], value)
	e.Raw(data[:])
}

func (e *Encoder) String(value string, max int) {
	if !utf8.ValidString(value) || len(value) > max || uint64(len(value)) > math.MaxUint32 {
		e.err = e.errors.LimitExceeded
		return
	}
	e.U32(uint32(len(value)))
	e.Raw([]byte(value))
}

// Finish returns the payload without a format-specific footer.
func (e *Encoder) Finish() ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}
	return append([]byte(nil), e.buffer.Bytes()...), nil
}

// Decoder reads a little-endian, versioned binary payload and leaves footer
// validation to the owning format.
type Decoder struct {
	data   []byte
	offset int
	limits Limits
	errors Errors
	err    error
}

func (d *Decoder) Err() error { return d.err }

func NewDecoder(data []byte, magic string, version uint16, limits Limits, errors Errors) (*Decoder, error) {
	errors = errors.withDefaults()
	if len(data) < 8 || string(data[:4]) != magic {
		return nil, errors.Corrupt
	}
	if binary.LittleEndian.Uint16(data[4:6]) != version {
		return nil, errors.UnsupportedVersion
	}
	if binary.LittleEndian.Uint16(data[6:8]) != 0 {
		return nil, errors.Corrupt
	}
	return &Decoder{data: data, offset: 8, limits: limits, errors: errors}, nil
}

func (d *Decoder) Take(size int) []byte {
	if d.err != nil || size < 0 || size > len(d.data)-d.offset {
		d.err = d.errors.Corrupt
		return nil
	}
	value := d.data[d.offset : d.offset+size]
	d.offset += size
	return value
}

func (d *Decoder) U8() uint8 {
	data := d.Take(1)
	if data == nil {
		return 0
	}
	return data[0]
}

func (d *Decoder) U16() uint16 {
	data := d.Take(2)
	if data == nil {
		return 0
	}
	return binary.LittleEndian.Uint16(data)
}

func (d *Decoder) U32() uint32 {
	data := d.Take(4)
	if data == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(data)
}

func (d *Decoder) U64() uint64 {
	data := d.Take(8)
	if data == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(data)
}

func (d *Decoder) String() string {
	size := uint64(d.U32())
	if d.err != nil {
		return ""
	}
	if size > uint64(d.limits.MaxStringBytes) {
		d.err = d.errors.LimitExceeded
		return ""
	}
	if size > uint64(len(d.data)-d.offset) {
		d.err = d.errors.Corrupt
		return ""
	}
	value := string(d.Take(int(size)))
	if !utf8.ValidString(value) {
		d.err = d.errors.Corrupt
		return ""
	}
	return value
}

func (d *Decoder) Done() error {
	if d.err != nil {
		return d.err
	}
	if d.offset != len(d.data) {
		return d.errors.Corrupt
	}
	return nil
}

func (d *Decoder) Remaining() int {
	if d.err != nil {
		return 0
	}
	return len(d.data) - d.offset
}
