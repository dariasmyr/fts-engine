package semanticpersist

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"unicode/utf8"
)

const formatFooterSize = 4

type encoder struct {
	buffer bytes.Buffer
	limit  uint64
	err    error
}

func newEncoder(magic string, version uint16, limit uint64) *encoder {
	e := &encoder{limit: limit}
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

func (e *encoder) finish() ([]byte, fileReference, error) {
	if e.err != nil {
		return nil, fileReference{}, e.err
	}
	checksum := crc32.ChecksumIEEE(e.buffer.Bytes())
	e.u32(checksum)
	if e.err != nil {
		return nil, fileReference{}, e.err
	}
	data := append([]byte(nil), e.buffer.Bytes()...)
	return data, fileReference{Size: uint64(len(data)), SHA256: sha256.Sum256(data)}, nil
}

type decoder struct {
	data   []byte
	offset int
	limits Limits
	err    error
}

func newDecoder(data []byte, magic string, version uint16, limits Limits) (*decoder, error) {
	if len(data) < 8+formatFooterSize || string(data[:4]) != magic {
		return nil, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(data[4:6]) != version {
		return nil, ErrUnsupportedVersion
	}
	if binary.LittleEndian.Uint16(data[6:8]) != 0 {
		return nil, ErrCorrupt
	}
	body := data[:len(data)-formatFooterSize]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(data[len(data)-formatFooterSize:]) {
		return nil, ErrCorrupt
	}
	return &decoder{data: body, offset: 8, limits: limits}, nil
}

func (d *decoder) take(size int) []byte {
	if d.err != nil || size < 0 || size > len(d.data)-d.offset {
		d.err = ErrCorrupt
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
	if d.err != nil {
		return ""
	}
	if size > uint64(d.limits.MaxStringBytes) {
		d.err = ErrLimitExceeded
		return ""
	}
	if size > uint64(len(d.data)-d.offset) {
		d.err = ErrCorrupt
		return ""
	}
	value := string(d.take(int(size)))
	if !utf8.ValidString(value) {
		d.err = ErrCorrupt
		return ""
	}
	return value
}

func (d *decoder) done() error {
	if d.err != nil {
		return d.err
	}
	if d.offset != len(d.data) {
		return ErrCorrupt
	}
	return nil
}

func (d *decoder) remaining() int {
	if d.err != nil {
		return 0
	}
	return len(d.data) - d.offset
}

func decodeBoundedInt(d *decoder) int {
	value := d.u64()
	if value > uint64(math.MaxInt) {
		d.err = ErrLimitExceeded
		return 0
	}
	return int(value)
}

func readBounded(reader io.Reader, limit uint64) ([]byte, error) {
	if reader == nil || limit == 0 || limit >= math.MaxInt64 {
		return nil, ErrLimitExceeded
	}
	data, err := io.ReadAll(io.LimitReader(reader, int64(limit)+1))
	if err != nil {
		return nil, fmt.Errorf("semanticpersist: read: %w", err)
	}
	if uint64(len(data)) > limit {
		return nil, ErrLimitExceeded
	}
	return data, nil
}

func normalizeLimits(limits Limits) Limits {
	defaults := DefaultLimits()
	if limits.MaxFileBytes == 0 {
		limits.MaxFileBytes = defaults.MaxFileBytes
	}
	if limits.MaxVectorBytes == 0 {
		limits.MaxVectorBytes = defaults.MaxVectorBytes
	}
	if limits.MaxGraphBytes == 0 {
		limits.MaxGraphBytes = defaults.MaxGraphBytes
	}
	if limits.MaxGraphLinks == 0 {
		limits.MaxGraphLinks = defaults.MaxGraphLinks
	}
	if limits.MaxOpenBytes == 0 {
		limits.MaxOpenBytes = defaults.MaxOpenBytes
	}
	if limits.MaxEfSearch == 0 {
		limits.MaxEfSearch = defaults.MaxEfSearch
	}
	if limits.MaxVisitLimit == 0 {
		limits.MaxVisitLimit = defaults.MaxVisitLimit
	}
	if limits.MaxDimensions == 0 {
		limits.MaxDimensions = defaults.MaxDimensions
	}
	if limits.MaxVectors == 0 {
		limits.MaxVectors = defaults.MaxVectors
	}
	if limits.MaxDocuments == 0 {
		limits.MaxDocuments = defaults.MaxDocuments
	}
	if limits.MaxStringBytes == 0 {
		limits.MaxStringBytes = defaults.MaxStringBytes
	}
	if limits.MaxChunksPerDocument == 0 {
		limits.MaxChunksPerDocument = defaults.MaxChunksPerDocument
	}
	if limits.MaxK == 0 {
		limits.MaxK = defaults.MaxK
	}
	return limits
}

func validateLimits(limits Limits) error {
	if limits.MaxFileBytes == 0 || limits.MaxFileBytes >= math.MaxInt64 || limits.MaxVectorBytes == 0 || limits.MaxVectorBytes > math.MaxUint64-128 ||
		limits.MaxGraphBytes == 0 || limits.MaxGraphBytes >= math.MaxInt64 || limits.MaxGraphLinks == 0 ||
		limits.MaxOpenBytes < limits.MaxFileBytes || limits.MaxOpenBytes >= math.MaxInt64 || limits.MaxEfSearch <= 0 || limits.MaxVisitLimit <= 0 ||
		limits.MaxDimensions <= 0 || limits.MaxVectors <= 0 || limits.MaxDocuments <= 0 || limits.MaxStringBytes <= 0 ||
		limits.MaxChunksPerDocument <= 0 || limits.MaxK <= 0 || uint64(limits.MaxVectors) >= math.MaxUint32 ||
		uint64(limits.MaxDocuments) > math.MaxUint32 || uint64(limits.MaxStringBytes) > math.MaxUint32 ||
		uint64(limits.MaxChunksPerDocument) > math.MaxUint32 || uint64(limits.MaxK) > math.MaxUint32 {
		return ErrLimitExceeded
	}
	return nil
}

func checkedAdd64(a, b uint64) (uint64, bool) {
	if b > math.MaxUint64-a {
		return 0, false
	}
	return a + b, true
}

func checkedMultiply64(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}
