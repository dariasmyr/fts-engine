package flat

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

const (
	codecMagic      = "VFLT"
	codecVersion    = uint16(1)
	codecHeaderSize = 40
	codecFooterSize = 4
)

var (
	ErrCorruptSegment   = errors.New("vector/flat: corrupt segment")
	ErrUnsupportedCodec = errors.New("vector/flat: unsupported segment codec")
	ErrSegmentLimit     = errors.New("vector/flat: segment exceeds configured limit")
)

type CodecLimits struct {
	MaxDimensions  int
	MaxVectors     int
	MaxVectorBytes uint64
	MaxK           int
}

func DefaultCodecLimits() CodecLimits {
	return CodecLimits{MaxDimensions: 65_536, MaxVectors: 10_000_000, MaxVectorBytes: 512 << 20, MaxK: 1_000_000}
}

type FileMetadata struct {
	Size   uint64
	CRC32  uint32
	SHA256 [sha256.Size]byte
}

func Marshal(reader *Searcher) ([]byte, FileMetadata, error) {
	var buffer bytes.Buffer
	metadata, err := Write(&buffer, reader)
	return buffer.Bytes(), metadata, err
}

// MarshalSource encodes any immutable prepared vector source as VFLT data.
func MarshalSource(source vector.PreparedVectorSource, maxK int) ([]byte, FileMetadata, error) {
	var buffer bytes.Buffer
	metadata, err := WriteSource(&buffer, source, maxK)
	return buffer.Bytes(), metadata, err
}

// Write streams one immutable fixed-width matrix and its checksum.
func Write(writer io.Writer, reader *Searcher) (FileMetadata, error) {
	if reader == nil {
		return FileMetadata{}, ErrCorruptSegment
	}
	return WriteSource(writer, reader.VectorSource(), reader.maxK)
}

// WriteSource streams an immutable prepared vector source as fixed-width VFLT
// data. The source remains the caller's responsibility and is not retained.
func WriteSource(writer io.Writer, source vector.PreparedVectorSource, maxK int) (FileMetadata, error) {
	if writer == nil || source == nil || source.Dimensions() <= 0 || maxK <= 0 || source.Len() < 0 {
		return FileMetadata{}, ErrCorruptSegment
	}
	space, err := vector.NewSpace(source.Dimensions(), source.Metric())
	if err != nil || space.Normalization() != source.Normalization() {
		return FileMetadata{}, ErrCorruptSegment
	}
	if uint64(maxK) > math.MaxUint32 || uint64(source.Len()) >= math.MaxUint32 || uint64(source.Dimensions()) > math.MaxUint32 {
		return FileMetadata{}, ErrSegmentLimit
	}
	components, ok := checkedMultiply(uint64(source.Len()), uint64(source.Dimensions()))
	if !ok || components > uint64(math.MaxInt) {
		return FileMetadata{}, ErrSegmentLimit
	}
	vectorBytes := components * 4
	if vectorBytes > math.MaxUint64-uint64(codecHeaderSize+codecFooterSize) {
		return FileMetadata{}, ErrSegmentLimit
	}
	scratch := make([]float32, source.Dimensions())
	for row := range source.Len() {
		for i := range scratch {
			scratch[i] = float32(math.NaN())
		}
		if err := source.ReadVectorInto(context.Background(), vector.Ordinal(row), scratch); err != nil {
			return FileMetadata{}, fmt.Errorf("%w: row %d: %v", ErrCorruptSegment, row, err)
		}
		if err := validatePreparedRow(space, scratch); err != nil {
			return FileMetadata{}, fmt.Errorf("%w: row %d: %v", ErrCorruptSegment, row, err)
		}
	}
	header := make([]byte, codecHeaderSize)
	copy(header[:4], codecMagic)
	binary.LittleEndian.PutUint16(header[4:6], codecVersion)
	binary.LittleEndian.PutUint16(header[6:8], codecHeaderSize)
	binary.LittleEndian.PutUint32(header[8:12], uint32(source.Dimensions()))
	header[12] = byte(source.Metric())
	header[13] = byte(source.Normalization())
	binary.LittleEndian.PutUint32(header[16:20], uint32(source.Len()))
	binary.LittleEndian.PutUint32(header[20:24], uint32(maxK))
	binary.LittleEndian.PutUint64(header[24:32], vectorBytes)

	crc := crc32.NewIEEE()
	identity := sha256.New()
	body := io.MultiWriter(writer, crc, identity)
	if err := writeAll(body, header); err != nil {
		return FileMetadata{}, fmt.Errorf("vector/flat: write header: %w", err)
	}
	rowBytes := source.Dimensions() * 4
	buffer := make([]byte, rowBytes)
	for row := range source.Len() {
		if err := source.ReadVectorInto(context.Background(), vector.Ordinal(row), scratch); err != nil {
			return FileMetadata{}, fmt.Errorf("%w: row %d: %v", ErrCorruptSegment, row, err)
		}
		encoded := buffer[:rowBytes]
		for i, value := range scratch {
			binary.LittleEndian.PutUint32(encoded[i*4:(i+1)*4], math.Float32bits(value))
		}
		if err := writeAll(body, encoded); err != nil {
			return FileMetadata{}, fmt.Errorf("vector/flat: write vectors: %w", err)
		}
	}
	checksum := crc.Sum32()
	var footer [codecFooterSize]byte
	binary.LittleEndian.PutUint32(footer[:], checksum)
	if err := writeAll(io.MultiWriter(writer, identity), footer[:]); err != nil {
		return FileMetadata{}, fmt.Errorf("vector/flat: write checksum: %w", err)
	}
	metadata := FileMetadata{Size: codecHeaderSize + vectorBytes + codecFooterSize, CRC32: checksum}
	copy(metadata.SHA256[:], identity.Sum(nil))
	return metadata, nil
}

func Open(source io.Reader, limits CodecLimits) (*Searcher, FileMetadata, error) {
	if source == nil {
		return nil, FileMetadata{}, ErrCorruptSegment
	}
	limits = normalizeCodecLimits(limits)
	if limits.MaxDimensions <= 0 || limits.MaxVectors <= 0 || limits.MaxK <= 0 {
		return nil, FileMetadata{}, ErrSegmentLimit
	}
	maxFileBytes, ok := checkedAdd(uint64(codecHeaderSize+codecFooterSize), limits.MaxVectorBytes)
	if !ok || maxFileBytes >= math.MaxInt64 {
		return nil, FileMetadata{}, ErrSegmentLimit
	}
	data, err := io.ReadAll(io.LimitReader(source, int64(maxFileBytes)+1))
	if err != nil {
		return nil, FileMetadata{}, fmt.Errorf("vector/flat: read segment: %w", err)
	}
	if uint64(len(data)) > maxFileBytes {
		return nil, FileMetadata{}, ErrSegmentLimit
	}
	reader, metadata, err := openBytes(data, limits)
	return reader, metadata, err
}

func openBytes(data []byte, limits CodecLimits) (*Searcher, FileMetadata, error) {
	if len(data) < codecHeaderSize+codecFooterSize || string(data[:4]) != codecMagic {
		return nil, FileMetadata{}, ErrCorruptSegment
	}
	if binary.LittleEndian.Uint16(data[4:6]) != codecVersion {
		return nil, FileMetadata{}, ErrUnsupportedCodec
	}
	if binary.LittleEndian.Uint16(data[6:8]) != codecHeaderSize || data[14] != 0 || data[15] != 0 || binary.LittleEndian.Uint64(data[32:40]) != 0 {
		return nil, FileMetadata{}, ErrCorruptSegment
	}
	body, footer := data[:len(data)-codecFooterSize], data[len(data)-codecFooterSize:]
	checksum := binary.LittleEndian.Uint32(footer)
	if crc32.ChecksumIEEE(body) != checksum {
		return nil, FileMetadata{}, ErrCorruptSegment
	}
	dimensions := int(binary.LittleEndian.Uint32(data[8:12]))
	metric := vector.Metric(data[12])
	normalization := vector.Normalization(data[13])
	count := uint64(binary.LittleEndian.Uint32(data[16:20]))
	maxK := uint64(binary.LittleEndian.Uint32(data[20:24]))
	vectorBytes := binary.LittleEndian.Uint64(data[24:32])
	if dimensions <= 0 || dimensions > limits.MaxDimensions || count > uint64(limits.MaxVectors) || maxK == 0 || maxK > uint64(limits.MaxK) || vectorBytes > limits.MaxVectorBytes {
		return nil, FileMetadata{}, ErrSegmentLimit
	}
	expectedComponents, ok := checkedMultiply(count, uint64(dimensions))
	if !ok {
		return nil, FileMetadata{}, ErrSegmentLimit
	}
	expectedBytes, ok := checkedMultiply(expectedComponents, 4)
	if !ok || expectedBytes != vectorBytes {
		return nil, FileMetadata{}, ErrCorruptSegment
	}
	expectedSize, ok := checkedAdd(uint64(codecHeaderSize+codecFooterSize), vectorBytes)
	if !ok || expectedSize != uint64(len(data)) || expectedComponents > uint64(math.MaxInt) {
		return nil, FileMetadata{}, ErrCorruptSegment
	}
	space, err := vector.NewSpace(dimensions, metric)
	if err != nil || space.Normalization() != normalization {
		return nil, FileMetadata{}, ErrCorruptSegment
	}
	values := make([]float32, int(expectedComponents))
	payload := data[codecHeaderSize : len(data)-codecFooterSize]
	for i := range values {
		bits := binary.LittleEndian.Uint32(payload[i*4 : (i+1)*4])
		if bits == 1<<31 {
			return nil, FileMetadata{}, ErrCorruptSegment
		}
		values[i] = math.Float32frombits(bits)
	}
	for row := 0; row < int(count); row++ {
		start := row * dimensions
		value := values[start : start+dimensions]
		if err := validatePreparedRow(space, value); err != nil {
			return nil, FileMetadata{}, fmt.Errorf("%w: row %d: %v", ErrCorruptSegment, row, err)
		}
	}
	identity := sha256.Sum256(data)
	return newReader(space, int(maxK), values), FileMetadata{Size: uint64(len(data)), CRC32: checksum, SHA256: identity}, nil
}

func normalizeCodecLimits(limits CodecLimits) CodecLimits {
	defaults := DefaultCodecLimits()
	if limits.MaxDimensions == 0 {
		limits.MaxDimensions = defaults.MaxDimensions
	}
	if limits.MaxVectors == 0 {
		limits.MaxVectors = defaults.MaxVectors
	}
	if limits.MaxVectorBytes == 0 {
		limits.MaxVectorBytes = defaults.MaxVectorBytes
	}
	if limits.MaxK == 0 {
		limits.MaxK = defaults.MaxK
	}
	return limits
}

func validatePreparedRow(space vector.Space, value []float32) error {
	if err := space.Validate(value); err != nil {
		return err
	}
	if space.Normalization() == vector.NormalizationUnitLength {
		var normSquared float64
		for _, component := range value {
			normSquared += float64(component) * float64(component)
		}
		if math.Abs(normSquared-1) > 1e-4 {
			return errors.New("vector is not unit normalized")
		}
	}
	for _, component := range value {
		if math.Float32bits(component) == 1<<31 {
			return errors.New("vector contains negative zero")
		}
	}
	return nil
}

func checkedMultiply(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}

func checkedAdd(a, b uint64) (uint64, bool) {
	if b > math.MaxUint64-a {
		return 0, false
	}
	return a + b, true
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
