package semanticpersist

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
	codecMagic            = "VFLT"
	codecVersion          = uint16(1)
	codecHeaderSize       = 40
	codecFooterSize       = 4
	float32ByteSize       = 4
	unitNormTolerance     = 1e-4
	negativeZeroBits      = uint32(1) << 31
	defaultMaxDimensions  = 65_536
	defaultMaxVectors     = 10_000_000
	defaultMaxVectorBytes = 512 << 20
	defaultMaxK           = 1_000_000

	headerMagicOffset         = 0
	headerVersionOffset       = 4
	headerSizeOffset          = 6
	headerDimensionsOffset    = 8
	headerMetricOffset        = 12
	headerNormalizationOffset = 13
	headerReservedOffset      = 14
	headerCountOffset         = 16
	headerMaxKOffset          = 20
	headerVectorBytesOffset   = 24
	headerTailOffset          = 32
)

var (
	ErrCorruptSegment   = errors.New("semanticpersist: corrupt vector segment")
	ErrUnsupportedCodec = errors.New("semanticpersist: unsupported vector codec")
	ErrSegmentLimit     = errors.New("semanticpersist: vector segment exceeds configured limit")
)

type CodecLimits struct {
	MaxDimensions  int
	MaxVectors     int
	MaxVectorBytes uint64
	MaxK           int
}

func DefaultCodecLimits() CodecLimits {
	return CodecLimits{
		MaxDimensions:  defaultMaxDimensions,
		MaxVectors:     defaultMaxVectors,
		MaxVectorBytes: defaultMaxVectorBytes,
		MaxK:           defaultMaxK,
	}
}

type FileMetadata struct {
	Size   uint64
	CRC32  uint32
	SHA256 [sha256.Size]byte
}

func Marshal(source vector.PreparedVectorSource, maxK int) ([]byte, FileMetadata, error) {
	var buffer bytes.Buffer
	metadata, err := Write(&buffer, source, maxK)
	return buffer.Bytes(), metadata, err
}

// MarshalSource encodes any immutable prepared vector source as VFLT data.
func MarshalSource(source vector.PreparedVectorSource, maxK int) ([]byte, FileMetadata, error) {
	var buffer bytes.Buffer
	metadata, err := WriteSource(&buffer, source, maxK)
	return buffer.Bytes(), metadata, err
}

// Write streams one immutable fixed-width matrix and its checksum.
func Write(writer io.Writer, source vector.PreparedVectorSource, maxK int) (FileMetadata, error) {
	return WriteSource(writer, source, maxK)
}

// WriteSource streams an immutable prepared vector source as fixed-width VFLT
// data. The source remains the caller's responsibility and is not retained.
func WriteSource(writer io.Writer, source vector.PreparedVectorSource, maxK int) (FileMetadata, error) {
	if writer == nil || source == nil || source.Dimensions() <= 0 || maxK <= 0 || source.Len() < 0 {
		return FileMetadata{}, ErrCorruptSegment
	}
	space, err := vector.NewCalculator(source.Dimensions(), source.Metric())
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
	vectorBytes := components * float32ByteSize
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
	copy(header[headerMagicOffset:], codecMagic)
	binary.LittleEndian.PutUint16(header[headerVersionOffset:headerSizeOffset], codecVersion)
	binary.LittleEndian.PutUint16(header[headerSizeOffset:headerDimensionsOffset], codecHeaderSize)
	binary.LittleEndian.PutUint32(header[headerDimensionsOffset:headerMetricOffset], uint32(source.Dimensions()))
	header[headerMetricOffset] = byte(source.Metric())
	header[headerNormalizationOffset] = byte(source.Normalization())
	binary.LittleEndian.PutUint32(header[headerCountOffset:headerMaxKOffset], uint32(source.Len()))
	binary.LittleEndian.PutUint32(header[headerMaxKOffset:headerVectorBytesOffset], uint32(maxK))
	binary.LittleEndian.PutUint64(header[headerVectorBytesOffset:headerTailOffset], vectorBytes)

	crc := crc32.NewIEEE()
	identity := sha256.New()
	body := io.MultiWriter(writer, crc, identity)
	if err := writeAll(body, header); err != nil {
		return FileMetadata{}, fmt.Errorf("semanticpersist: write vector header: %w", err)
	}
	rowBytes := source.Dimensions() * float32ByteSize
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
			return FileMetadata{}, fmt.Errorf("semanticpersist: write vectors: %w", err)
		}
	}
	checksum := crc.Sum32()
	var footer [codecFooterSize]byte
	binary.LittleEndian.PutUint32(footer[:], checksum)
	if err := writeAll(io.MultiWriter(writer, identity), footer[:]); err != nil {
		return FileMetadata{}, fmt.Errorf("semanticpersist: write vector checksum: %w", err)
	}
	metadata := FileMetadata{Size: codecHeaderSize + vectorBytes + codecFooterSize, CRC32: checksum}
	copy(metadata.SHA256[:], identity.Sum(nil))
	return metadata, nil
}

func OpenVectorSource(source io.Reader, limits CodecLimits) (vector.PreparedVectorSource, FileMetadata, error) {
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
		return nil, FileMetadata{}, fmt.Errorf("semanticpersist: read vector segment: %w", err)
	}
	if uint64(len(data)) > maxFileBytes {
		return nil, FileMetadata{}, ErrSegmentLimit
	}
	reader, metadata, err := openBytes(data, limits)
	return reader, metadata, err
}

func openBytes(data []byte, limits CodecLimits) (vector.PreparedVectorSource, FileMetadata, error) {
	if len(data) < codecHeaderSize+codecFooterSize || string(data[headerMagicOffset:headerVersionOffset]) != codecMagic {
		return nil, FileMetadata{}, ErrCorruptSegment
	}
	if binary.LittleEndian.Uint16(data[headerVersionOffset:headerSizeOffset]) != codecVersion {
		return nil, FileMetadata{}, ErrUnsupportedCodec
	}
	if binary.LittleEndian.Uint16(data[headerSizeOffset:headerDimensionsOffset]) != codecHeaderSize ||
		data[headerReservedOffset] != 0 || data[headerReservedOffset+1] != 0 ||
		binary.LittleEndian.Uint64(data[headerTailOffset:codecHeaderSize]) != 0 {
		return nil, FileMetadata{}, ErrCorruptSegment
	}
	body, footer := data[:len(data)-codecFooterSize], data[len(data)-codecFooterSize:]
	checksum := binary.LittleEndian.Uint32(footer)
	if crc32.ChecksumIEEE(body) != checksum {
		return nil, FileMetadata{}, ErrCorruptSegment
	}
	dimensions := int(binary.LittleEndian.Uint32(data[headerDimensionsOffset:headerMetricOffset]))
	metric := vector.Metric(data[headerMetricOffset])
	normalization := vector.Normalization(data[headerNormalizationOffset])
	count := uint64(binary.LittleEndian.Uint32(data[headerCountOffset:headerMaxKOffset]))
	maxK := uint64(binary.LittleEndian.Uint32(data[headerMaxKOffset:headerVectorBytesOffset]))
	vectorBytes := binary.LittleEndian.Uint64(data[headerVectorBytesOffset:headerTailOffset])
	if dimensions <= 0 || dimensions > limits.MaxDimensions || count > uint64(limits.MaxVectors) || maxK == 0 || maxK > uint64(limits.MaxK) || vectorBytes > limits.MaxVectorBytes {
		return nil, FileMetadata{}, ErrSegmentLimit
	}
	expectedComponents, ok := checkedMultiply(count, uint64(dimensions))
	if !ok {
		return nil, FileMetadata{}, ErrSegmentLimit
	}
	expectedBytes, ok := checkedMultiply(expectedComponents, float32ByteSize)
	if !ok || expectedBytes != vectorBytes {
		return nil, FileMetadata{}, ErrCorruptSegment
	}
	expectedSize, ok := checkedAdd(uint64(codecHeaderSize+codecFooterSize), vectorBytes)
	if !ok || expectedSize != uint64(len(data)) || expectedComponents > uint64(math.MaxInt) {
		return nil, FileMetadata{}, ErrCorruptSegment
	}
	space, err := vector.NewCalculator(dimensions, metric)
	if err != nil || space.Normalization() != normalization {
		return nil, FileMetadata{}, ErrCorruptSegment
	}
	values := make([]float32, int(expectedComponents))
	payload := data[codecHeaderSize : len(data)-codecFooterSize]
	for i := range values {
		bits := binary.LittleEndian.Uint32(payload[i*float32ByteSize : (i+1)*float32ByteSize])
		if bits == negativeZeroBits {
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
	prepared, err := vector.NewPreparedMemorySource(space, values)
	if err != nil {
		return nil, FileMetadata{}, fmt.Errorf("%w: %v", ErrCorruptSegment, err)
	}
	return prepared, FileMetadata{Size: uint64(len(data)), CRC32: checksum, SHA256: identity}, nil
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

func validatePreparedRow(calculator vector.Calculator, value []float32) error {
	if err := calculator.Validate(value); err != nil {
		return err
	}
	if calculator.Normalization() == vector.NormalizationUnitLength {
		var normSquared float64
		for _, component := range value {
			normSquared += float64(component) * float64(component)
		}
		if math.Abs(normSquared-1) > unitNormTolerance {
			return errors.New("vector is not unit normalized")
		}
	}
	for _, component := range value {
		if math.Float32bits(component) == negativeZeroBits {
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
