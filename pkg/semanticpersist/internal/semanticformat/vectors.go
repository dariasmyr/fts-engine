package semanticformat

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
)

// VectorFileMetadata describes one encoded SVEC file.
type VectorFileMetadata struct {
	FileRef
	CRC32 uint32
}

// DecodedVectors is the in-memory payload reconstructed from an SVEC file.
type DecodedVectors struct {
	Calculator vector.Calculator
	Values     []float32
}

// WriteVectorFile writes one canonical SVEC file and returns its persisted identity.
func WriteVectorFile(
	ctx context.Context,
	writer io.Writer,
	source vector.PreparedVectorStore,
	maxK int,
	limits VectorLimits,
) (VectorFileMetadata, error) {
	if ctx == nil {
		return VectorFileMetadata{}, vector.ErrNilContext
	}
	if err := limits.validate(); err != nil {
		return VectorFileMetadata{}, err
	}
	if writer == nil || source == nil || source.Dimensions() <= 0 || source.Len() < 0 || maxK <= 0 {
		return VectorFileMetadata{}, codecErrorf(ErrCorrupt, "vector file source is invalid")
	}
	if source.Dimensions() > limits.MaxDimensions || source.Len() > limits.MaxVectors || maxK > limits.MaxK {
		return VectorFileMetadata{}, ErrLimitExceeded
	}
	if uint64(source.Dimensions()) > math.MaxUint32 || uint64(source.Len()) >= math.MaxUint32 || uint64(maxK) > math.MaxUint32 {
		return VectorFileMetadata{}, ErrLimitExceeded
	}

	calculator, err := vector.NewCalculator(source.Dimensions(), source.Metric())
	if err != nil || calculator.Normalization() != source.Normalization() {
		return VectorFileMetadata{}, codecErrorf(ErrCorrupt, "vector file calculator is invalid")
	}

	components, ok := checkedMultiply(uint64(source.Len()), uint64(source.Dimensions()))
	if !ok || components > uint64(math.MaxInt) {
		return VectorFileMetadata{}, ErrLimitExceeded
	}
	vectorBytes, ok := checkedMultiply(components, float32ByteSize)
	if !ok || vectorBytes > limits.MaxVectorBytes {
		return VectorFileMetadata{}, ErrLimitExceeded
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
		return VectorFileMetadata{}, fmt.Errorf("semanticformat: write vector header: %w", err)
	}

	scratch := make([]float32, source.Dimensions())
	rowBytes := source.Dimensions() * float32ByteSize
	encoded := make([]byte, rowBytes)
	for row := range source.Len() {
		if err := ctx.Err(); err != nil {
			return VectorFileMetadata{}, err
		}
		for i := range scratch {
			scratch[i] = float32(math.NaN())
		}
		if err := source.ReadVectorInto(ctx, vector.Ordinal(row), scratch); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return VectorFileMetadata{}, err
			}
			return VectorFileMetadata{}, codecErrorf(ErrCorrupt, "vector row %d cannot be read: %v", row, err)
		}
		if err := validatePreparedRow(calculator, scratch); err != nil {
			return VectorFileMetadata{}, codecErrorf(ErrCorrupt, "vector row %d is invalid: %v", row, err)
		}
		for i, value := range scratch {
			binary.LittleEndian.PutUint32(encoded[i*float32ByteSize:(i+1)*float32ByteSize], math.Float32bits(value))
		}
		if err := writeAll(body, encoded); err != nil {
			return VectorFileMetadata{}, fmt.Errorf("semanticformat: write vectors: %w", err)
		}
	}

	checksum := crc.Sum32()
	var footer [vectorFooterSize]byte
	binary.LittleEndian.PutUint32(footer[:], checksum)
	if err := writeAll(io.MultiWriter(writer, identity), footer[:]); err != nil {
		return VectorFileMetadata{}, fmt.Errorf("semanticformat: write vector checksum: %w", err)
	}

	totalSize, ok := checkedAdd(uint64(vectorHeaderSize+vectorFooterSize), vectorBytes)
	if !ok {
		return VectorFileMetadata{}, ErrLimitExceeded
	}
	metadata := VectorFileMetadata{FileRef: FileRef{Size: totalSize}, CRC32: checksum}
	copy(metadata.SHA256[:], identity.Sum(nil))
	return metadata, nil
}

// DecodeVectorFile validates and decodes one complete SVEC file.
func DecodeVectorFile(data []byte, limits VectorLimits) (DecodedVectors, VectorFileMetadata, error) {
	if err := limits.validate(); err != nil {
		return DecodedVectors{}, VectorFileMetadata{}, err
	}
	if len(data) < vectorHeaderSize+vectorFooterSize || string(data[vectorHeaderMagicOffset:vectorHeaderVersionOffset]) != vectorMagic {
		return DecodedVectors{}, VectorFileMetadata{}, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(data[vectorHeaderVersionOffset:vectorHeaderSizeOffset]) != vectorVersion {
		return DecodedVectors{}, VectorFileMetadata{}, ErrUnsupportedVersion
	}
	if binary.LittleEndian.Uint16(data[vectorHeaderSizeOffset:vectorHeaderDimensionsOffset]) != vectorHeaderSize ||
		data[vectorHeaderReservedOffset] != 0 || data[vectorHeaderReservedOffset+1] != 0 ||
		binary.LittleEndian.Uint64(data[vectorHeaderTailOffset:vectorHeaderSize]) != 0 {
		return DecodedVectors{}, VectorFileMetadata{}, ErrCorrupt
	}

	body := data[:len(data)-vectorFooterSize]
	footer := data[len(data)-vectorFooterSize:]
	checksum := binary.LittleEndian.Uint32(footer)
	if crc32.ChecksumIEEE(body) != checksum {
		return DecodedVectors{}, VectorFileMetadata{}, ErrCorrupt
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
		return DecodedVectors{}, VectorFileMetadata{}, ErrLimitExceeded
	}

	expectedComponents, ok := checkedMultiply(count, uint64(dimensions))
	if !ok || expectedComponents > uint64(math.MaxInt) {
		return DecodedVectors{}, VectorFileMetadata{}, ErrLimitExceeded
	}
	expectedBytes, ok := checkedMultiply(expectedComponents, float32ByteSize)
	if !ok {
		return DecodedVectors{}, VectorFileMetadata{}, ErrLimitExceeded
	}
	if expectedBytes != vectorBytes {
		return DecodedVectors{}, VectorFileMetadata{}, ErrCorrupt
	}
	expectedSize, ok := checkedAdd(uint64(vectorHeaderSize+vectorFooterSize), vectorBytes)
	if !ok {
		return DecodedVectors{}, VectorFileMetadata{}, ErrLimitExceeded
	}
	if expectedSize != uint64(len(data)) {
		return DecodedVectors{}, VectorFileMetadata{}, ErrCorrupt
	}

	calculator, err := vector.NewCalculator(dimensions, metric)
	if err != nil || calculator.Normalization() != normalization {
		return DecodedVectors{}, VectorFileMetadata{}, ErrCorrupt
	}

	values := make([]float32, int(expectedComponents))
	payload := data[vectorHeaderSize : len(data)-vectorFooterSize]
	for i := range values {
		bits := binary.LittleEndian.Uint32(payload[i*float32ByteSize : (i+1)*float32ByteSize])
		if bits == negativeZeroBits {
			return DecodedVectors{}, VectorFileMetadata{}, ErrCorrupt
		}
		values[i] = math.Float32frombits(bits)
	}
	for row := 0; row < int(count); row++ {
		start := row * dimensions
		if err := validatePreparedRow(calculator, values[start:start+dimensions]); err != nil {
			return DecodedVectors{}, VectorFileMetadata{}, codecErrorf(ErrCorrupt, "vector row %d is invalid: %v", row, err)
		}
	}

	metadata := VectorFileMetadata{
		FileRef: FileRef{Size: uint64(len(data)), SHA256: sha256.Sum256(data)},
		CRC32:   checksum,
	}
	return DecodedVectors{Calculator: calculator, Values: values}, metadata, nil
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
