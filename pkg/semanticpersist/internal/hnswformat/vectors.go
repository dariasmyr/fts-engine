package hnswformat

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/v2/internal/format"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

const (
	vectorMagic      = "SVEC"
	vectorVersion    = uint16(2)
	vectorHeaderSize = 40
	vectorFooterSize = 4
	float32ByteSize  = 4

	vectorHeaderMagicOffset         = 0
	vectorHeaderVersionOffset       = 4
	vectorHeaderSizeOffset          = 6
	vectorHeaderDimensionsOffset    = 8
	vectorHeaderMetricOffset        = 12
	vectorHeaderNormalizationOffset = 13
	vectorHeaderReservedOffset      = 14
	vectorHeaderCountOffset         = 16
	vectorHeaderMaxKOffset          = 20
	vectorHeaderVectorBytesOffset   = 24
	vectorHeaderTailOffset          = 32

	unitNormTolerance = 1e-4
	negativeZeroBits  = uint32(1) << 31
)

// VectorData is the in-memory payload reconstructed from an SVEC file.
type VectorData struct {
	Space  vector.Space
	Values []float32
}

// WriteVectorFile writes one canonical SVEC file and returns its persisted identity.
func WriteVectorFile(
	ctx context.Context,
	writer io.Writer,
	source vector.PreparedVectorStore,
	maxK int,
	limits Config,
) (FileMetadata, error) {
	if ctx == nil {
		return FileMetadata{}, vector.ErrNilContext
	}
	if err := limits.validate(); err != nil {
		return FileMetadata{}, err
	}
	if writer == nil || source == nil || source.Dimensions() <= 0 || source.Len() < 0 || maxK <= 0 {
		return FileMetadata{}, codecErrorf(ErrCorrupt, "vector file source is invalid")
	}
	if source.Dimensions() > limits.MaxDimensions || source.Len() > limits.MaxVectors || maxK > limits.MaxK {
		return FileMetadata{}, ErrLimitExceeded
	}
	if uint64(source.Dimensions()) > math.MaxUint32 || uint64(source.Len()) >= math.MaxUint32 || uint64(maxK) > math.MaxUint32 {
		return FileMetadata{}, ErrLimitExceeded
	}

	space, err := vector.NewSpace(source.Dimensions(), source.Metric())
	if err != nil || space.Normalization() != source.Normalization() {
		return FileMetadata{}, codecErrorf(ErrCorrupt, "vector file space is invalid")
	}

	components, ok := format.CheckedMultiplyUint64(uint64(source.Len()), uint64(source.Dimensions()))
	if !ok || components > uint64(math.MaxInt) {
		return FileMetadata{}, ErrLimitExceeded
	}
	vectorBytes, ok := format.CheckedMultiplyUint64(components, float32ByteSize)
	if !ok || vectorBytes > limits.MaxVectorBytes {
		return FileMetadata{}, ErrLimitExceeded
	}

	header := make([]byte, vectorHeaderSize)
	copy(header[vectorHeaderMagicOffset:], vectorMagic)
	binary.LittleEndian.PutUint16(header[vectorHeaderVersionOffset:vectorHeaderSizeOffset], vectorVersion)
	binary.LittleEndian.PutUint16(header[vectorHeaderSizeOffset:vectorHeaderDimensionsOffset], vectorHeaderSize)
	binary.LittleEndian.PutUint32(header[vectorHeaderDimensionsOffset:vectorHeaderMetricOffset], uint32(source.Dimensions()))
	header[vectorHeaderMetricOffset] = byte(source.Metric())
	header[vectorHeaderNormalizationOffset] = byte(source.Normalization())
	binary.LittleEndian.PutUint32(header[vectorHeaderCountOffset:vectorHeaderMaxKOffset], uint32(source.Len()))
	binary.LittleEndian.PutUint32(header[vectorHeaderMaxKOffset:vectorHeaderVectorBytesOffset], uint32(maxK))
	binary.LittleEndian.PutUint64(header[vectorHeaderVectorBytesOffset:vectorHeaderTailOffset], vectorBytes)

	crc := crc32.NewIEEE()
	identity := sha256.New()
	body := io.MultiWriter(writer, crc, identity)
	if err := writeAll(body, header); err != nil {
		return FileMetadata{}, fmt.Errorf("hnswformat: write vector header: %w", err)
	}

	scratch := make([]float32, source.Dimensions())
	rowBytes := source.Dimensions() * float32ByteSize
	encoded := make([]byte, rowBytes)
	for row := range source.Len() {
		if err := ctx.Err(); err != nil {
			return FileMetadata{}, err
		}
		for i := range scratch {
			scratch[i] = float32(math.NaN())
		}
		if err := source.ReadVectorInto(ctx, vector.Ordinal(row), scratch); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return FileMetadata{}, err
			}
			return FileMetadata{}, codecErrorf(ErrCorrupt, "vector row %d cannot be read: %v", row, err)
		}
		if err := validatePreparedRow(space, scratch); err != nil {
			return FileMetadata{}, codecErrorf(ErrCorrupt, "vector row %d is invalid: %v", row, err)
		}
		for i, value := range scratch {
			binary.LittleEndian.PutUint32(encoded[i*float32ByteSize:(i+1)*float32ByteSize], math.Float32bits(value))
		}
		if err := writeAll(body, encoded); err != nil {
			return FileMetadata{}, fmt.Errorf("hnswformat: write vectors: %w", err)
		}
	}

	checksum := crc.Sum32()
	var footer [vectorFooterSize]byte
	binary.LittleEndian.PutUint32(footer[:], checksum)
	if err := writeAll(io.MultiWriter(writer, identity), footer[:]); err != nil {
		return FileMetadata{}, fmt.Errorf("hnswformat: write vector checksum: %w", err)
	}

	totalSize, ok := format.CheckedAddUint64(uint64(vectorHeaderSize+vectorFooterSize), vectorBytes)
	if !ok {
		return FileMetadata{}, ErrLimitExceeded
	}
	metadata := FileMetadata{FileRef: FileRef{Size: totalSize}, CRC32: checksum}
	copy(metadata.SHA256[:], identity.Sum(nil))
	return metadata, nil
}

// DecodeVectorFile validates and decodes one complete SVEC file.
func DecodeVectorFile(ctx context.Context, data []byte, limits Config) (VectorData, FileMetadata, error) {
	if ctx == nil {
		return VectorData{}, FileMetadata{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return VectorData{}, FileMetadata{}, err
	}
	if err := limits.validate(); err != nil {
		return VectorData{}, FileMetadata{}, err
	}
	if len(data) < vectorHeaderSize+vectorFooterSize || string(data[vectorHeaderMagicOffset:vectorHeaderVersionOffset]) != vectorMagic {
		return VectorData{}, FileMetadata{}, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(data[vectorHeaderVersionOffset:vectorHeaderSizeOffset]) != vectorVersion {
		return VectorData{}, FileMetadata{}, ErrUnsupportedVersion
	}
	if binary.LittleEndian.Uint16(data[vectorHeaderSizeOffset:vectorHeaderDimensionsOffset]) != vectorHeaderSize ||
		data[vectorHeaderReservedOffset] != 0 || data[vectorHeaderReservedOffset+1] != 0 ||
		binary.LittleEndian.Uint64(data[vectorHeaderTailOffset:vectorHeaderSize]) != 0 {
		return VectorData{}, FileMetadata{}, ErrCorrupt
	}

	body := data[:len(data)-vectorFooterSize]
	footer := data[len(data)-vectorFooterSize:]
	checksum := binary.LittleEndian.Uint32(footer)
	actualChecksum, identity, err := vectorFileHashes(ctx, body, footer)
	if err != nil {
		return VectorData{}, FileMetadata{}, err
	}
	if actualChecksum != checksum {
		return VectorData{}, FileMetadata{}, ErrCorrupt
	}

	dimensions := int(binary.LittleEndian.Uint32(data[vectorHeaderDimensionsOffset:vectorHeaderMetricOffset]))
	metric := vector.Metric(data[vectorHeaderMetricOffset])
	normalization := vector.Normalization(data[vectorHeaderNormalizationOffset])
	count := uint64(binary.LittleEndian.Uint32(data[vectorHeaderCountOffset:vectorHeaderMaxKOffset]))
	maxK := uint64(binary.LittleEndian.Uint32(data[vectorHeaderMaxKOffset:vectorHeaderVectorBytesOffset]))
	vectorBytes := binary.LittleEndian.Uint64(data[vectorHeaderVectorBytesOffset:vectorHeaderTailOffset])
	if dimensions <= 0 || dimensions > limits.MaxDimensions ||
		count > uint64(limits.MaxVectors) ||
		maxK == 0 || maxK > uint64(limits.MaxK) ||
		vectorBytes > limits.MaxVectorBytes {
		return VectorData{}, FileMetadata{}, ErrLimitExceeded
	}

	expectedComponents, ok := format.CheckedMultiplyUint64(count, uint64(dimensions))
	if !ok || expectedComponents > uint64(math.MaxInt) {
		return VectorData{}, FileMetadata{}, ErrLimitExceeded
	}
	expectedBytes, ok := format.CheckedMultiplyUint64(expectedComponents, float32ByteSize)
	if !ok {
		return VectorData{}, FileMetadata{}, ErrLimitExceeded
	}
	if expectedBytes != vectorBytes {
		return VectorData{}, FileMetadata{}, ErrCorrupt
	}
	expectedSize, ok := format.CheckedAddUint64(uint64(vectorHeaderSize+vectorFooterSize), vectorBytes)
	if !ok {
		return VectorData{}, FileMetadata{}, ErrLimitExceeded
	}
	if expectedSize != uint64(len(data)) {
		return VectorData{}, FileMetadata{}, ErrCorrupt
	}

	space, err := vector.NewSpace(dimensions, metric)
	if err != nil || space.Normalization() != normalization {
		return VectorData{}, FileMetadata{}, ErrCorrupt
	}

	values := make([]float32, int(expectedComponents))
	payload := data[vectorHeaderSize : len(data)-vectorFooterSize]
	for i := range values {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return VectorData{}, FileMetadata{}, err
		}
		bits := binary.LittleEndian.Uint32(payload[i*float32ByteSize : (i+1)*float32ByteSize])
		if bits == negativeZeroBits {
			return VectorData{}, FileMetadata{}, ErrCorrupt
		}
		values[i] = math.Float32frombits(bits)
	}
	for row := 0; row < int(count); row++ {
		if err := contextcheck.PeriodicError(ctx, row); err != nil {
			return VectorData{}, FileMetadata{}, err
		}
		start := row * dimensions
		if err := validatePreparedRow(space, values[start:start+dimensions]); err != nil {
			return VectorData{}, FileMetadata{}, codecErrorf(ErrCorrupt, "vector row %d is invalid: %v", row, err)
		}
	}

	metadata := FileMetadata{
		FileRef: FileRef{Size: uint64(len(data)), SHA256: identity},
		CRC32:   checksum,
	}
	if err := ctx.Err(); err != nil {
		return VectorData{}, FileMetadata{}, err
	}
	return VectorData{Space: space, Values: values}, metadata, nil
}

func vectorFileHashes(ctx context.Context, body, footer []byte) (uint32, [sha256.Size]byte, error) {
	const chunkSize = 64 << 10

	crc := crc32.NewIEEE()
	identity := sha256.New()
	for offset := 0; offset < len(body); offset += chunkSize {
		if err := ctx.Err(); err != nil {
			return 0, [sha256.Size]byte{}, err
		}
		end := min(offset+chunkSize, len(body))
		_, _ = crc.Write(body[offset:end])
		_, _ = identity.Write(body[offset:end])
	}
	if err := ctx.Err(); err != nil {
		return 0, [sha256.Size]byte{}, err
	}
	_, _ = identity.Write(footer)
	var digest [sha256.Size]byte
	copy(digest[:], identity.Sum(nil))
	return crc.Sum32(), digest, ctx.Err()
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
