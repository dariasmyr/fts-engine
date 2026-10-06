package format

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"unicode/utf8"

	internalformat "github.com/dariasmyr/fts-engine/internal/format"
)

var (
	ErrCorrupt            = errorString("semanticpersist/format: corrupt data")
	ErrUnsupportedVersion = errorString("semanticpersist/format: unsupported version")
	ErrLimitExceeded      = errorString("semanticpersist/format: configured limit exceeded")
)

const (
	// Metric or normalization.
	wireUint8Size = 1
	// Format version or reserved flags.
	wireUint16Size = 2
	// String length or collection count.
	wireUint32Size = 4
	// Vector ID, revision, or byte offset.
	wireUint64Size = 8
	// "SSTA", "SMAN", or "SCUR".
	wireMagicSize = 4
	// Magic, version, and reserved flags.
	wireHeaderSize = wireMagicSize + wireUint16Size + wireUint16Size
	// Trailing IEEE CRC32.
	wireChecksumSize = internalformat.CRC32Size
	// Byte length before a string, e.g. 5 before "model".
	wireStringLengthPrefixSize = wireUint32Size
)

type errorString string

func (e errorString) Error() string { return string(e) }

type categorizedError struct {
	category error
	detail   string
}

func (e categorizedError) Error() string { return e.detail }

func (e categorizedError) Is(target error) bool { return target == e.category }

func codecErrorf(category error, format string, args ...any) error {
	return categorizedError{category: category, detail: fmt.Sprintf(format, args...)}
}

// Limits bounds semantic persistence codec input and output.
type Limits struct {
	MaxFileBytes         uint64
	MaxDimensions        int
	MaxVectors           int
	MaxDocuments         int
	MaxStringBytes       int
	MaxChunksPerDocument int
	MaxK                 int
	MaxVectorBytes       uint64
	MaxEfSearch          int
	MaxVisitLimit        int
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

func newEncoder(magic string, version uint16, expectedSize uint64, limits Limits) *encoder {
	e := &encoder{limit: limits.MaxFileBytes}
	if expectedSize > limits.MaxFileBytes {
		e.err = ErrLimitExceeded
		return e
	}
	if expectedSize <= uint64(math.MaxInt) {
		e.buffer.Grow(int(expectedSize))
	}
	e.writeBytes([]byte(magic))
	e.writeUint16(version)
	e.writeUint16(0)
	return e
}

func (e *encoder) writeBytes(value []byte) {
	if e.err != nil {
		return
	}
	if uint64(len(value)) > e.limit-uint64(e.buffer.Len()) {
		e.err = ErrLimitExceeded
		return
	}
	_, e.err = e.buffer.Write(value)
}

func (e *encoder) writeUint8(value uint8) { e.writeBytes([]byte{value}) }

func (e *encoder) writeUint16(value uint16) {
	var data [wireUint16Size]byte
	binary.LittleEndian.PutUint16(data[:], value)
	e.writeBytes(data[:])
}

func (e *encoder) writeUint32(value uint32) {
	var data [wireUint32Size]byte
	binary.LittleEndian.PutUint32(data[:], value)
	e.writeBytes(data[:])
}

func (e *encoder) writeUint64(value uint64) {
	var data [wireUint64Size]byte
	binary.LittleEndian.PutUint64(data[:], value)
	e.writeBytes(data[:])
}

func (e *encoder) writeString(value string, max int) {
	if !utf8.ValidString(value) || len(value) > max || uint64(len(value)) > math.MaxUint32 {
		e.err = ErrLimitExceeded
		return
	}
	e.writeUint32(uint32(len(value)))
	e.writeBytes([]byte(value))
}

func (e *encoder) finish() ([]byte, FileReference, error) {
	if e.err != nil {
		return nil, FileReference{}, e.err
	}
	checksum := internalformat.CRC32(e.buffer.Bytes())
	e.writeUint32(checksum)
	if e.err != nil {
		return nil, FileReference{}, e.err
	}
	data := e.buffer.Bytes()
	return data, FileReference{Size: uint64(len(data)), SHA256: sha256.Sum256(data)}, nil
}

type decoder struct {
	data           []byte
	offset         int
	maxStringBytes int
	decodeErr      error
}

func newDecoder(data []byte, magic string, version uint16, limits Limits) (*decoder, error) {
	if len(data) < wireHeaderSize+wireChecksumSize {
		return nil, ErrCorrupt
	}
	body := data[:len(data)-wireChecksumSize]
	if string(body[:wireMagicSize]) != magic {
		return nil, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(body[wireMagicSize:wireMagicSize+wireUint16Size]) != version {
		return nil, ErrUnsupportedVersion
	}
	if binary.LittleEndian.Uint16(body[wireMagicSize+wireUint16Size:wireHeaderSize]) != 0 {
		return nil, ErrCorrupt
	}
	checksum, ok := internalformat.ReadCRC32(data[len(data)-wireChecksumSize:])
	if !ok || internalformat.CRC32(body) != checksum {
		return nil, ErrCorrupt
	}
	return &decoder{data: body, offset: wireHeaderSize, maxStringBytes: limits.MaxStringBytes}, nil
}

func (d *decoder) readBytes(size int) []byte {
	if d.decodeErr != nil || size < 0 || size > len(d.data)-d.offset {
		d.decodeErr = ErrCorrupt
		return nil
	}
	value := d.data[d.offset : d.offset+size]
	d.offset += size
	return value
}

func (d *decoder) readUint8() uint8 {
	data := d.readBytes(wireUint8Size)
	if data == nil {
		return 0
	}
	return data[0]
}

func (d *decoder) readUint16() uint16 {
	data := d.readBytes(wireUint16Size)
	if data == nil {
		return 0
	}
	return binary.LittleEndian.Uint16(data)
}

func (d *decoder) readUint32() uint32 {
	data := d.readBytes(wireUint32Size)
	if data == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(data)
}

func (d *decoder) readUint64() uint64 {
	data := d.readBytes(wireUint64Size)
	if data == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(data)
}

func (d *decoder) readString() string {
	size := uint64(d.readUint32())
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
	value := string(d.readBytes(int(size)))
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

func addEncodedSize(total *uint64, size uint64) bool {
	if size > math.MaxUint64-*total {
		return false
	}
	*total += size
	return true
}

func addEncodedStringSize(total *uint64, value string) bool {
	return addEncodedSize(total, wireStringLengthPrefixSize+uint64(len(value)))
}
