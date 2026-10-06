package semanticpersist

import (
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
	codecMagic            = "SVEC"
	codecVersion          = uint16(2)
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
	errCorruptVectorFile     = errors.New("semanticpersist: corrupt vector file")
	errUnsupportedVectorFile = errors.New("semanticpersist: unsupported vector file")
	errVectorFileLimit       = errors.New("semanticpersist: vector file exceeds configured limit")
)

type codecLimitsConfig struct {
	MaxDimensions  int
	MaxVectors     int
	MaxVectorBytes uint64
	MaxK           int
}

func defaultCodecLimits() codecLimitsConfig {
	return codecLimitsConfig{
		MaxDimensions:  defaultMaxDimensions,
		MaxVectors:     defaultMaxVectors,
		MaxVectorBytes: defaultMaxVectorBytes,
		MaxK:           defaultMaxK,
	}
}

type vectorFileMetadata struct {
	Size   uint64
	CRC32  uint32
	SHA256 [sha256.Size]byte
}

type preparedVectors struct {
	Calculator vector.Calculator
	Values     []float32
}

func writeVectorFile(ctx context.Context, writer io.Writer, source vector.PreparedVectorStore, maxK int, validateRows bool) (vectorFileMetadata, error) {
	if ctx == nil {
		return vectorFileMetadata{}, vector.ErrNilContext
	}
	if writer == nil || source == nil || source.Dimensions() <= 0 || maxK <= 0 || source.Len() < 0 {
		return vectorFileMetadata{}, errCorruptVectorFile
	}
	space, err := vector.NewCalculator(source.Dimensions(), source.Metric())
	if err != nil || space.Normalization() != source.Normalization() {
		return vectorFileMetadata{}, errCorruptVectorFile
	}
	if uint64(maxK) > math.MaxUint32 || uint64(source.Len()) >= math.MaxUint32 || uint64(source.Dimensions()) > math.MaxUint32 {
		return vectorFileMetadata{}, errVectorFileLimit
	}
	components, ok := checkedMultiply(uint64(source.Len()), uint64(source.Dimensions()))
	if !ok || components > uint64(math.MaxInt) {
		return vectorFileMetadata{}, errVectorFileLimit
	}
	vectorBytes := components * float32ByteSize
	if vectorBytes > math.MaxUint64-uint64(codecHeaderSize+codecFooterSize) {
		return vectorFileMetadata{}, errVectorFileLimit
	}
	scratch := make([]float32, source.Dimensions())
	if validateRows {
		for row := range source.Len() {
			if err := ctx.Err(); err != nil {
				return vectorFileMetadata{}, err
			}
			for i := range scratch {
				scratch[i] = float32(math.NaN())
			}
			if err := source.ReadVectorInto(ctx, vector.Ordinal(row), scratch); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return vectorFileMetadata{}, err
				}
				return vectorFileMetadata{}, fmt.Errorf("%w: row %d: %v", errCorruptVectorFile, row, err)
			}
			if err := validatePreparedRow(space, scratch); err != nil {
				return vectorFileMetadata{}, fmt.Errorf("%w: row %d: %v", errCorruptVectorFile, row, err)
			}
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
		return vectorFileMetadata{}, fmt.Errorf("semanticpersist: write vector header: %w", err)
	}
	rowBytes := source.Dimensions() * float32ByteSize
	buffer := make([]byte, rowBytes)
	for row := range source.Len() {
		if err := ctx.Err(); err != nil {
			return vectorFileMetadata{}, err
		}
		if err := source.ReadVectorInto(ctx, vector.Ordinal(row), scratch); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return vectorFileMetadata{}, err
			}
			return vectorFileMetadata{}, fmt.Errorf("%w: row %d: %v", errCorruptVectorFile, row, err)
		}
		encoded := buffer[:rowBytes]
		for i, value := range scratch {
			binary.LittleEndian.PutUint32(encoded[i*4:(i+1)*4], math.Float32bits(value))
		}
		if err := writeAll(body, encoded); err != nil {
			return vectorFileMetadata{}, fmt.Errorf("semanticpersist: write vectors: %w", err)
		}
	}
	checksum := crc.Sum32()
	var footer [codecFooterSize]byte
	binary.LittleEndian.PutUint32(footer[:], checksum)
	if err := writeAll(io.MultiWriter(writer, identity), footer[:]); err != nil {
		return vectorFileMetadata{}, fmt.Errorf("semanticpersist: write vector checksum: %w", err)
	}
	metadata := vectorFileMetadata{Size: codecHeaderSize + vectorBytes + codecFooterSize, CRC32: checksum}
	copy(metadata.SHA256[:], identity.Sum(nil))
	return metadata, nil
}

func openBytes(data []byte, limits codecLimitsConfig) (preparedVectors, vectorFileMetadata, error) {
	if len(data) < codecHeaderSize+codecFooterSize || string(data[headerMagicOffset:headerVersionOffset]) != codecMagic {
		return preparedVectors{}, vectorFileMetadata{}, errCorruptVectorFile
	}
	if binary.LittleEndian.Uint16(data[headerVersionOffset:headerSizeOffset]) != codecVersion {
		return preparedVectors{}, vectorFileMetadata{}, errUnsupportedVectorFile
	}
	if binary.LittleEndian.Uint16(data[headerSizeOffset:headerDimensionsOffset]) != codecHeaderSize ||
		data[headerReservedOffset] != 0 || data[headerReservedOffset+1] != 0 ||
		binary.LittleEndian.Uint64(data[headerTailOffset:codecHeaderSize]) != 0 {
		return preparedVectors{}, vectorFileMetadata{}, errCorruptVectorFile
	}
	body, footer := data[:len(data)-codecFooterSize], data[len(data)-codecFooterSize:]
	checksum := binary.LittleEndian.Uint32(footer)
	if crc32.ChecksumIEEE(body) != checksum {
		return preparedVectors{}, vectorFileMetadata{}, errCorruptVectorFile
	}
	dimensions := int(binary.LittleEndian.Uint32(data[headerDimensionsOffset:headerMetricOffset]))
	metric := vector.Metric(data[headerMetricOffset])
	normalization := vector.Normalization(data[headerNormalizationOffset])
	count := uint64(binary.LittleEndian.Uint32(data[headerCountOffset:headerMaxKOffset]))
	maxK := uint64(binary.LittleEndian.Uint32(data[headerMaxKOffset:headerVectorBytesOffset]))
	vectorBytes := binary.LittleEndian.Uint64(data[headerVectorBytesOffset:headerTailOffset])
	if dimensions <= 0 || dimensions > limits.MaxDimensions || count > uint64(limits.MaxVectors) || maxK == 0 || maxK > uint64(limits.MaxK) || vectorBytes > limits.MaxVectorBytes {
		return preparedVectors{}, vectorFileMetadata{}, errVectorFileLimit
	}
	expectedComponents, ok := checkedMultiply(count, uint64(dimensions))
	if !ok {
		return preparedVectors{}, vectorFileMetadata{}, errVectorFileLimit
	}
	expectedBytes, ok := checkedMultiply(expectedComponents, float32ByteSize)
	if !ok || expectedBytes != vectorBytes {
		return preparedVectors{}, vectorFileMetadata{}, errCorruptVectorFile
	}
	expectedSize, ok := checkedAdd(uint64(codecHeaderSize+codecFooterSize), vectorBytes)
	if !ok || expectedSize != uint64(len(data)) || expectedComponents > uint64(math.MaxInt) {
		return preparedVectors{}, vectorFileMetadata{}, errCorruptVectorFile
	}
	space, err := vector.NewCalculator(dimensions, metric)
	if err != nil || space.Normalization() != normalization {
		return preparedVectors{}, vectorFileMetadata{}, errCorruptVectorFile
	}
	values := make([]float32, int(expectedComponents))
	payload := data[codecHeaderSize : len(data)-codecFooterSize]
	for i := range values {
		bits := binary.LittleEndian.Uint32(payload[i*float32ByteSize : (i+1)*float32ByteSize])
		if bits == negativeZeroBits {
			return preparedVectors{}, vectorFileMetadata{}, errCorruptVectorFile
		}
		values[i] = math.Float32frombits(bits)
	}
	for row := 0; row < int(count); row++ {
		start := row * dimensions
		value := values[start : start+dimensions]
		if err := validatePreparedRow(space, value); err != nil {
			return preparedVectors{}, vectorFileMetadata{}, fmt.Errorf("%w: row %d: %v", errCorruptVectorFile, row, err)
		}
	}
	identity := sha256.Sum256(data)
	metadata := vectorFileMetadata{Size: uint64(len(data)), CRC32: checksum, SHA256: identity}
	return preparedVectors{Calculator: space, Values: values}, metadata, nil

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
