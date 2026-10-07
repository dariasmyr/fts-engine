// Package semanticformat encodes and decodes semantic persistence files.
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
)

const (
	Magic         = "VHNG"
	Version       = uint16(2)
	HeaderSize    = 128
	FooterSize    = 4
	maxReadBuffer = 64 << 10
)

var ErrCorruptData = errors.New("semanticformat: corrupt graph data")

// Reference identifies the authoritative vector file associated with a graph.
type Reference struct {
	Size   uint64
	SHA256 [sha256.Size]byte
}

// Metadata describes one encoded VHNG file.
type Metadata struct {
	Size   uint64
	CRC32  uint32
	SHA256 [sha256.Size]byte
}

// BuildInfo is persisted graph-construction provenance.
type BuildInfo struct {
	BuildVersion          uint32
	LevelGeneratorVersion uint32
	MaxNeighbors          uint32
	LevelZeroMaxNeighbors uint32
	EfConstruction        uint32
	Seed                  uint64
}

// Graph is the wire-level VHNG DTO. Runtime search policy is intentionally not
// persisted. Node ordinal is identical to vector ordinal, so no node mapping is
// stored either.
type Graph struct {
	Dimensions    uint32
	Metric        uint8
	Normalization uint8

	HasEntry bool
	Entry    uint32
	MaxLevel uint8

	Build   BuildInfo
	Vectors Reference

	Levels           []uint8
	Level0Offsets    []uint32
	Level0Links      []uint32
	UpperNodeOffsets []uint32
	UpperLinkOffsets []uint32
	UpperLinks       []uint32
}

// Limits bounds decoding. Zero values select safe defaults.
type Limits struct {
	MaxGraphBytes uint64
	MaxVectors    uint64
	MaxLinks      uint64
	MaxDimensions uint64
	MaxLevel      uint64
}

func DefaultLimits() Limits {
	return Limits{
		MaxGraphBytes: 1 << 30,
		MaxVectors:    10_000_000,
		MaxLinks:      100_000_000,
		MaxDimensions: 65_536,
		MaxLevel:      63,
	}
}

func Encode(writer io.Writer, graph Graph) (Metadata, error) {
	if writer == nil || !validReference(graph.Vectors) || graph.Dimensions == 0 ||
		uint64(len(graph.Levels)) >= math.MaxUint32 || len(graph.Level0Offsets) != len(graph.Levels)+1 ||
		len(graph.UpperNodeOffsets) != len(graph.Levels)+1 || len(graph.UpperLinkOffsets) == 0 ||
		uint64(len(graph.Level0Links)) >= math.MaxUint32 || uint64(len(graph.UpperLinkOffsets)-1) >= math.MaxUint32 ||
		uint64(len(graph.UpperLinks)) >= math.MaxUint32 {
		return Metadata{}, ErrCorruptData
	}
	if !graph.HasEntry && (graph.Entry != 0 || len(graph.Levels) != 0) ||
		graph.HasEntry && uint64(graph.Entry) >= uint64(len(graph.Levels)) {
		return Metadata{}, ErrCorruptData
	}
	if graph.Build.BuildVersion == 0 || graph.Build.LevelGeneratorVersion == 0 ||
		graph.Build.MaxNeighbors < 2 || graph.Build.LevelZeroMaxNeighbors != graph.Build.MaxNeighbors*2 ||
		graph.Build.EfConstruction < graph.Build.MaxNeighbors {
		return Metadata{}, ErrCorruptData
	}

	fileSize, err := EncodedSize(graph)
	if err != nil {
		return Metadata{}, err
	}
	topologyBytes := fileSize - HeaderSize - FooterSize

	header := make([]byte, HeaderSize)
	copy(header[:4], Magic)
	binary.LittleEndian.PutUint16(header[4:6], Version)
	binary.LittleEndian.PutUint16(header[6:8], HeaderSize)
	binary.LittleEndian.PutUint32(header[8:12], graph.Dimensions)
	header[12], header[13] = graph.Metric, graph.Normalization
	if graph.HasEntry {
		header[14] = 1
	}
	binary.LittleEndian.PutUint32(header[16:20], uint32(len(graph.Levels)))
	binary.LittleEndian.PutUint32(header[20:24], graph.Entry)
	putBuildInfo(header[24:52], graph.Build)
	binary.LittleEndian.PutUint64(header[56:64], graph.Vectors.Size)
	copy(header[64:96], graph.Vectors.SHA256[:])
	binary.LittleEndian.PutUint32(header[96:100], uint32(len(graph.Level0Links)))
	binary.LittleEndian.PutUint32(header[100:104], uint32(len(graph.UpperLinkOffsets)-1))
	binary.LittleEndian.PutUint32(header[104:108], uint32(len(graph.UpperLinks)))
	maxLevel := graph.MaxLevel
	if len(graph.Levels) == 0 {
		maxLevel = 0xff
	}
	header[108] = maxLevel
	binary.LittleEndian.PutUint64(header[112:120], topologyBytes)
	binary.LittleEndian.PutUint64(header[120:128], fileSize)

	crc, identity := crc32.NewIEEE(), sha256.New()
	body := io.MultiWriter(writer, crc, identity)
	if err := writeBytes(body, header); err != nil {
		return Metadata{}, fmt.Errorf("semanticformat: write graph header: %w", err)
	}
	if err := writeBytes(body, graph.Levels); err != nil {
		return Metadata{}, fmt.Errorf("semanticformat: write node levels: %w", err)
	}
	if padding := int(aligned4(uint64(len(graph.Levels)))) - len(graph.Levels); padding != 0 {
		if err := writeBytes(body, make([]byte, padding)); err != nil {
			return Metadata{}, fmt.Errorf("semanticformat: write level padding: %w", err)
		}
	}

	buffer := make([]byte, 64<<10)
	for _, section := range [][]uint32{
		graph.Level0Offsets,
		graph.Level0Links,
		graph.UpperNodeOffsets,
		graph.UpperLinkOffsets,
		graph.UpperLinks,
	} {
		if err := writeUint32s(body, section, buffer); err != nil {
			return Metadata{}, fmt.Errorf("semanticformat: write graph topology: %w", err)
		}
	}

	checksum := crc.Sum32()
	var footer [FooterSize]byte
	binary.LittleEndian.PutUint32(footer[:], checksum)
	if err := writeBytes(io.MultiWriter(writer, identity), footer[:]); err != nil {
		return Metadata{}, fmt.Errorf("semanticformat: write graph checksum: %w", err)
	}
	var digest [sha256.Size]byte
	copy(digest[:], identity.Sum(nil))
	return Metadata{Size: fileSize, CRC32: checksum, SHA256: digest}, nil
}

func DecodeContext(ctx context.Context, source io.Reader, limits Limits) (Graph, Metadata, error) {
	if ctx == nil {
		return Graph{}, Metadata{}, context.Canceled
	}
	if source == nil {
		return Graph{}, Metadata{}, ErrCorruptData
	}
	if err := ctx.Err(); err != nil {
		return Graph{}, Metadata{}, err
	}
	limits = normalizeLimits(limits)
	if limits.MaxGraphBytes < HeaderSize+FooterSize || limits.MaxGraphBytes >= math.MaxInt64 {
		return Graph{}, Metadata{}, ErrLimitExceeded
	}
	data, err := io.ReadAll(io.LimitReader(contextReader{ctx: ctx, reader: source}, int64(limits.MaxGraphBytes)+1))
	if err != nil {
		return Graph{}, Metadata{}, fmt.Errorf("semanticformat: read graph: %w", err)
	}
	if uint64(len(data)) > limits.MaxGraphBytes {
		return Graph{}, Metadata{}, ErrLimitExceeded
	}
	return DecodeBytesContext(ctx, data, limits)
}

func DecodeBytesContext(ctx context.Context, data []byte, limits Limits) (Graph, Metadata, error) {
	if ctx == nil {
		return Graph{}, Metadata{}, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return Graph{}, Metadata{}, err
	}
	limits = normalizeLimits(limits)
	if uint64(len(data)) > limits.MaxGraphBytes {
		return Graph{}, Metadata{}, ErrLimitExceeded
	}
	if len(data) < HeaderSize+FooterSize || string(data[:4]) != Magic {
		return Graph{}, Metadata{}, ErrCorruptData
	}
	if binary.LittleEndian.Uint16(data[4:6]) != Version {
		return Graph{}, Metadata{}, ErrUnsupportedVersion
	}
	if binary.LittleEndian.Uint16(data[6:8]) != HeaderSize || data[15] != 0 || !zero(data[52:56]) || !zero(data[109:112]) {
		return Graph{}, Metadata{}, ErrCorruptData
	}

	checksum := binary.LittleEndian.Uint32(data[len(data)-FooterSize:])
	crc, identity := crc32.NewIEEE(), sha256.New()
	for offset := 0; offset < len(data); offset += maxReadBuffer {
		if err := ctx.Err(); err != nil {
			return Graph{}, Metadata{}, err
		}
		end := min(offset+maxReadBuffer, len(data))
		_, _ = identity.Write(data[offset:end])
		if offset < len(data)-FooterSize {
			_, _ = crc.Write(data[offset:min(end, len(data)-FooterSize)])
		}
	}
	if checksum != crc.Sum32() {
		return Graph{}, Metadata{}, ErrCorruptData
	}

	graph := Graph{
		Dimensions:    binary.LittleEndian.Uint32(data[8:12]),
		Metric:        data[12],
		Normalization: data[13],
		HasEntry:      data[14] == 1,
		Entry:         binary.LittleEndian.Uint32(data[20:24]),
		Build:         getBuildInfo(data[24:52]),
		Vectors:       Reference{Size: binary.LittleEndian.Uint64(data[56:64])},
		MaxLevel:      data[108],
	}
	copy(graph.Vectors.SHA256[:], data[64:96])
	if data[14] > 1 || !validReference(graph.Vectors) {
		return Graph{}, Metadata{}, ErrCorruptData
	}

	nodes := uint64(binary.LittleEndian.Uint32(data[16:20]))
	level0 := uint64(binary.LittleEndian.Uint32(data[96:100]))
	placements := uint64(binary.LittleEndian.Uint32(data[100:104]))
	upper := uint64(binary.LittleEndian.Uint32(data[104:108]))
	if nodes > limits.MaxVectors || graph.Dimensions == 0 || uint64(graph.Dimensions) > limits.MaxDimensions ||
		level0 > limits.MaxLinks || upper > limits.MaxLinks || level0+upper > limits.MaxLinks {
		return Graph{}, Metadata{}, ErrLimitExceeded
	}
	if nodes == 0 {
		if graph.HasEntry || graph.Entry != 0 || graph.MaxLevel != 0xff {
			return Graph{}, Metadata{}, ErrCorruptData
		}
	} else if !graph.HasEntry || uint64(graph.Entry) >= nodes || uint64(graph.MaxLevel) > limits.MaxLevel {
		return Graph{}, Metadata{}, ErrCorruptData
	}

	topo, ok := topologyBytes(nodes, level0, placements, upper)
	if !ok || binary.LittleEndian.Uint64(data[112:120]) != topo || binary.LittleEndian.Uint64(data[120:128]) != uint64(len(data)) {
		return Graph{}, Metadata{}, ErrCorruptData
	}
	if uint64(len(data)) != HeaderSize+topo+FooterSize {
		return Graph{}, Metadata{}, ErrCorruptData
	}

	graph.Levels = make([]uint8, int(nodes))
	graph.Level0Offsets = make([]uint32, int(nodes)+1)
	graph.Level0Links = make([]uint32, int(level0))
	graph.UpperNodeOffsets = make([]uint32, int(nodes)+1)
	graph.UpperLinkOffsets = make([]uint32, int(placements)+1)
	graph.UpperLinks = make([]uint32, int(upper))

	offset := HeaderSize
	copy(graph.Levels, data[offset:offset+int(nodes)])
	offset += int(aligned4(nodes))
	if !zero(data[HeaderSize+int(nodes) : offset]) {
		return Graph{}, Metadata{}, ErrCorruptData
	}
	var err error
	for _, section := range []*[]uint32{
		&graph.Level0Offsets,
		&graph.Level0Links,
		&graph.UpperNodeOffsets,
		&graph.UpperLinkOffsets,
		&graph.UpperLinks,
	} {
		offset, err = readUint32s(data, offset, *section)
		if err != nil {
			return Graph{}, Metadata{}, err
		}
	}
	if offset != len(data)-FooterSize {
		return Graph{}, Metadata{}, ErrCorruptData
	}

	var digest [sha256.Size]byte
	copy(digest[:], identity.Sum(nil))
	return graph, Metadata{Size: uint64(len(data)), CRC32: checksum, SHA256: digest}, nil
}

func EncodedSize(graph Graph) (uint64, error) {
	if len(graph.UpperLinkOffsets) == 0 {
		return 0, ErrCorruptData
	}
	topologyBytes, ok := topologyBytes(
		uint64(len(graph.Levels)),
		uint64(len(graph.Level0Links)),
		uint64(len(graph.UpperLinkOffsets)-1),
		uint64(len(graph.UpperLinks)),
	)
	if !ok {
		return 0, ErrLimitExceeded
	}
	fileSize, ok := checkedAdd(HeaderSize+FooterSize, topologyBytes)
	if !ok {
		return 0, ErrLimitExceeded
	}
	return fileSize, nil
}

func topologyBytes(nodes, level0, placements, upper uint64) (uint64, bool) {
	words, ok := checkedAdd(nodes+1, level0)
	if !ok {
		return 0, false
	}
	for _, n := range []uint64{nodes + 1, placements + 1, upper} {
		words, ok = checkedAdd(words, n)
		if !ok {
			return 0, false
		}
	}
	bytes, ok := checkedMultiply(words, 4)
	if !ok {
		return 0, false
	}
	return checkedAdd(bytes, aligned4(nodes))
}

func normalizeLimits(l Limits) Limits {
	d := DefaultLimits()
	if l.MaxGraphBytes == 0 {
		l.MaxGraphBytes = d.MaxGraphBytes
	}
	if l.MaxVectors == 0 {
		l.MaxVectors = d.MaxVectors
	}
	if l.MaxLinks == 0 {
		l.MaxLinks = d.MaxLinks
	}
	if l.MaxDimensions == 0 {
		l.MaxDimensions = d.MaxDimensions
	}
	if l.MaxLevel == 0 {
		l.MaxLevel = d.MaxLevel
	}
	return l
}

func validReference(r Reference) bool {
	return r.Size != 0 && r.SHA256 != [sha256.Size]byte{}
}

func aligned4(v uint64) uint64 { return (v + 3) &^ 3 }

func zero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}

func putBuildInfo(dst []byte, info BuildInfo) {
	binary.LittleEndian.PutUint32(dst[0:4], info.BuildVersion)
	binary.LittleEndian.PutUint32(dst[4:8], info.LevelGeneratorVersion)
	binary.LittleEndian.PutUint32(dst[8:12], info.MaxNeighbors)
	binary.LittleEndian.PutUint32(dst[12:16], info.LevelZeroMaxNeighbors)
	binary.LittleEndian.PutUint32(dst[16:20], info.EfConstruction)
	binary.LittleEndian.PutUint64(dst[20:28], info.Seed)
}

func getBuildInfo(src []byte) BuildInfo {
	return BuildInfo{
		BuildVersion:          binary.LittleEndian.Uint32(src[0:4]),
		LevelGeneratorVersion: binary.LittleEndian.Uint32(src[4:8]),
		MaxNeighbors:          binary.LittleEndian.Uint32(src[8:12]),
		LevelZeroMaxNeighbors: binary.LittleEndian.Uint32(src[12:16]),
		EfConstruction:        binary.LittleEndian.Uint32(src[16:20]),
		Seed:                  binary.LittleEndian.Uint64(src[20:28]),
	}
}

func writeBytes(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func writeUint32s(writer io.Writer, values []uint32, buffer []byte) error {
	for offset := 0; offset < len(values); {
		n := min(len(buffer)/4, len(values)-offset)
		for i := 0; i < n; i++ {
			binary.LittleEndian.PutUint32(buffer[i*4:], values[offset+i])
		}
		if err := writeBytes(writer, buffer[:n*4]); err != nil {
			return err
		}
		offset += n
	}
	return nil
}

func readUint32s(data []byte, offset int, values []uint32) (int, error) {
	end := offset + len(values)*4
	if offset < 0 || end < offset || end > len(data) {
		return 0, ErrCorruptData
	}
	for i := range values {
		values[i] = binary.LittleEndian.Uint32(data[offset+i*4:])
	}
	return end, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
