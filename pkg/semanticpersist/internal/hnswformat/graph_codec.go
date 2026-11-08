package hnswformat

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/dariasmyr/fts-engine/v2/internal/format"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

const (
	graphMagic      = "VHNG"
	graphVersion    = uint16(2)
	graphHeaderSize = 128
	graphFooterSize = 4
	maxReadBuffer   = 64 << 10
)

type graphBuildInfo struct {
	BuildVersion          uint32
	LevelGeneratorVersion uint32
	MaxNeighbors          uint32
	LevelZeroMaxNeighbors uint32
	EfConstruction        uint32
	Seed                  uint64
}

type graph struct {
	Dimensions    uint32
	Metric        uint8
	Normalization uint8

	HasEntry bool
	Entry    uint32
	MaxLevel uint8

	Build   graphBuildInfo
	Vectors FileRef

	Levels           []uint8
	Level0Offsets    []uint32
	Level0Links      []uint32
	UpperNodeOffsets []uint32
	UpperLinkOffsets []uint32
	UpperLinks       []uint32
}

type graphLimits struct {
	MaxGraphBytes uint64
	MaxVectors    uint64
	MaxLinks      uint64
	MaxDimensions uint64
	MaxLevel      uint64
}

func encodeGraph(writer io.Writer, graph graph) (FileMetadata, error) {
	if writer == nil || !graph.Vectors.Valid() || graph.Dimensions == 0 ||
		uint64(len(graph.Levels)) >= math.MaxUint32 || len(graph.Level0Offsets) != len(graph.Levels)+1 ||
		len(graph.UpperNodeOffsets) != len(graph.Levels)+1 || len(graph.UpperLinkOffsets) == 0 ||
		uint64(len(graph.Level0Links)) >= math.MaxUint32 || uint64(len(graph.UpperLinkOffsets)-1) >= math.MaxUint32 ||
		uint64(len(graph.UpperLinks)) >= math.MaxUint32 {
		return FileMetadata{}, ErrCorrupt
	}
	if !graph.HasEntry && (graph.Entry != 0 || len(graph.Levels) != 0) ||
		graph.HasEntry && uint64(graph.Entry) >= uint64(len(graph.Levels)) {
		return FileMetadata{}, ErrCorrupt
	}
	if graph.Build.BuildVersion == 0 || graph.Build.LevelGeneratorVersion == 0 ||
		graph.Build.MaxNeighbors < 2 || graph.Build.LevelZeroMaxNeighbors != graph.Build.MaxNeighbors*2 ||
		graph.Build.EfConstruction < graph.Build.MaxNeighbors {
		return FileMetadata{}, ErrCorrupt
	}

	fileSize, err := encodedGraphSize(graph)
	if err != nil {
		return FileMetadata{}, err
	}
	topologyBytes := fileSize - graphHeaderSize - graphFooterSize

	header := make([]byte, graphHeaderSize)
	copy(header[:4], graphMagic)
	binary.LittleEndian.PutUint16(header[4:6], graphVersion)
	binary.LittleEndian.PutUint16(header[6:8], graphHeaderSize)
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
	if err := writeAll(body, header); err != nil {
		return FileMetadata{}, fmt.Errorf("hnswformat: write graph header: %w", err)
	}
	if err := writeAll(body, graph.Levels); err != nil {
		return FileMetadata{}, fmt.Errorf("hnswformat: write node levels: %w", err)
	}
	if padding := int(aligned4(uint64(len(graph.Levels)))) - len(graph.Levels); padding != 0 {
		if err := writeAll(body, make([]byte, padding)); err != nil {
			return FileMetadata{}, fmt.Errorf("hnswformat: write level padding: %w", err)
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
			return FileMetadata{}, fmt.Errorf("hnswformat: write graph topology: %w", err)
		}
	}

	checksum := crc.Sum32()
	var footer [graphFooterSize]byte
	binary.LittleEndian.PutUint32(footer[:], checksum)
	if err := writeAll(io.MultiWriter(writer, identity), footer[:]); err != nil {
		return FileMetadata{}, fmt.Errorf("hnswformat: write graph checksum: %w", err)
	}
	var digest [sha256.Size]byte
	copy(digest[:], identity.Sum(nil))
	return FileMetadata{FileRef: FileRef{Size: fileSize, SHA256: digest}, CRC32: checksum}, nil
}

func decodeGraph(ctx context.Context, data []byte, limits graphLimits) (graph, FileMetadata, error) {
	if ctx == nil {
		return graph{}, FileMetadata{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return graph{}, FileMetadata{}, err
	}
	if limits.MaxGraphBytes == 0 || limits.MaxVectors == 0 || limits.MaxLinks == 0 ||
		limits.MaxDimensions == 0 || limits.MaxLevel == 0 {
		return graph{}, FileMetadata{}, ErrLimitExceeded
	}
	if uint64(len(data)) > limits.MaxGraphBytes {
		return graph{}, FileMetadata{}, ErrLimitExceeded
	}
	if len(data) < graphHeaderSize+graphFooterSize || string(data[:4]) != graphMagic {
		return graph{}, FileMetadata{}, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(data[4:6]) != graphVersion {
		return graph{}, FileMetadata{}, ErrUnsupportedVersion
	}
	if binary.LittleEndian.Uint16(data[6:8]) != graphHeaderSize || data[15] != 0 || !zero(data[52:56]) || !zero(data[109:112]) {
		return graph{}, FileMetadata{}, ErrCorrupt
	}

	checksum := binary.LittleEndian.Uint32(data[len(data)-graphFooterSize:])
	crc, identity := crc32.NewIEEE(), sha256.New()
	for offset := 0; offset < len(data); offset += maxReadBuffer {
		if err := ctx.Err(); err != nil {
			return graph{}, FileMetadata{}, err
		}
		end := min(offset+maxReadBuffer, len(data))
		_, _ = identity.Write(data[offset:end])
		if offset < len(data)-graphFooterSize {
			_, _ = crc.Write(data[offset:min(end, len(data)-graphFooterSize)])
		}
	}
	if checksum != crc.Sum32() {
		return graph{}, FileMetadata{}, ErrCorrupt
	}

	decoded := graph{
		Dimensions:    binary.LittleEndian.Uint32(data[8:12]),
		Metric:        data[12],
		Normalization: data[13],
		HasEntry:      data[14] == 1,
		Entry:         binary.LittleEndian.Uint32(data[20:24]),
		Build:         getBuildInfo(data[24:52]),
		Vectors:       FileRef{Size: binary.LittleEndian.Uint64(data[56:64])},
		MaxLevel:      data[108],
	}
	copy(decoded.Vectors.SHA256[:], data[64:96])
	if data[14] > 1 || !decoded.Vectors.Valid() {
		return graph{}, FileMetadata{}, ErrCorrupt
	}

	nodes := uint64(binary.LittleEndian.Uint32(data[16:20]))
	level0 := uint64(binary.LittleEndian.Uint32(data[96:100]))
	placements := uint64(binary.LittleEndian.Uint32(data[100:104]))
	upper := uint64(binary.LittleEndian.Uint32(data[104:108]))
	if nodes > limits.MaxVectors || decoded.Dimensions == 0 || uint64(decoded.Dimensions) > limits.MaxDimensions ||
		level0 > limits.MaxLinks || upper > limits.MaxLinks || level0+upper > limits.MaxLinks {
		return graph{}, FileMetadata{}, ErrLimitExceeded
	}
	if nodes == 0 {
		if decoded.HasEntry || decoded.Entry != 0 || decoded.MaxLevel != 0xff {
			return graph{}, FileMetadata{}, ErrCorrupt
		}
	} else if !decoded.HasEntry || uint64(decoded.Entry) >= nodes || uint64(decoded.MaxLevel) > limits.MaxLevel {
		return graph{}, FileMetadata{}, ErrCorrupt
	}

	topo, ok := topologyBytes(nodes, level0, placements, upper)
	if !ok || binary.LittleEndian.Uint64(data[112:120]) != topo || binary.LittleEndian.Uint64(data[120:128]) != uint64(len(data)) {
		return graph{}, FileMetadata{}, ErrCorrupt
	}
	if uint64(len(data)) != graphHeaderSize+topo+graphFooterSize {
		return graph{}, FileMetadata{}, ErrCorrupt
	}

	decoded.Levels = make([]uint8, int(nodes))
	decoded.Level0Offsets = make([]uint32, int(nodes)+1)
	decoded.Level0Links = make([]uint32, int(level0))
	decoded.UpperNodeOffsets = make([]uint32, int(nodes)+1)
	decoded.UpperLinkOffsets = make([]uint32, int(placements)+1)
	decoded.UpperLinks = make([]uint32, int(upper))

	offset := graphHeaderSize
	copy(decoded.Levels, data[offset:offset+int(nodes)])
	offset += int(aligned4(nodes))
	if !zero(data[graphHeaderSize+int(nodes) : offset]) {
		return graph{}, FileMetadata{}, ErrCorrupt
	}
	var err error
	for _, section := range []*[]uint32{
		&decoded.Level0Offsets,
		&decoded.Level0Links,
		&decoded.UpperNodeOffsets,
		&decoded.UpperLinkOffsets,
		&decoded.UpperLinks,
	} {
		offset, err = readUint32s(data, offset, *section)
		if err != nil {
			return graph{}, FileMetadata{}, err
		}
	}
	if offset != len(data)-graphFooterSize {
		return graph{}, FileMetadata{}, ErrCorrupt
	}

	var digest [sha256.Size]byte
	copy(digest[:], identity.Sum(nil))
	return decoded, FileMetadata{
		FileRef: FileRef{Size: uint64(len(data)), SHA256: digest},
		CRC32:   checksum,
	}, nil
}

func encodedGraphSize(graph graph) (uint64, error) {
	if len(graph.UpperLinkOffsets) == 0 {
		return 0, ErrCorrupt
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
	fileSize, ok := format.CheckedAddUint64(graphHeaderSize+graphFooterSize, topologyBytes)
	if !ok {
		return 0, ErrLimitExceeded
	}
	return fileSize, nil
}

func topologyBytes(nodes, level0, placements, upper uint64) (uint64, bool) {
	words, ok := format.CheckedAddUint64(nodes+1, level0)
	if !ok {
		return 0, false
	}
	for _, n := range []uint64{nodes + 1, placements + 1, upper} {
		words, ok = format.CheckedAddUint64(words, n)
		if !ok {
			return 0, false
		}
	}
	bytes, ok := format.CheckedMultiplyUint64(words, 4)
	if !ok {
		return 0, false
	}
	return format.CheckedAddUint64(bytes, aligned4(nodes))
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

func putBuildInfo(dst []byte, info graphBuildInfo) {
	binary.LittleEndian.PutUint32(dst[0:4], info.BuildVersion)
	binary.LittleEndian.PutUint32(dst[4:8], info.LevelGeneratorVersion)
	binary.LittleEndian.PutUint32(dst[8:12], info.MaxNeighbors)
	binary.LittleEndian.PutUint32(dst[12:16], info.LevelZeroMaxNeighbors)
	binary.LittleEndian.PutUint32(dst[16:20], info.EfConstruction)
	binary.LittleEndian.PutUint64(dst[20:28], info.Seed)
}

func getBuildInfo(src []byte) graphBuildInfo {
	return graphBuildInfo{
		BuildVersion:          binary.LittleEndian.Uint32(src[0:4]),
		LevelGeneratorVersion: binary.LittleEndian.Uint32(src[4:8]),
		MaxNeighbors:          binary.LittleEndian.Uint32(src[8:12]),
		LevelZeroMaxNeighbors: binary.LittleEndian.Uint32(src[12:16]),
		EfConstruction:        binary.LittleEndian.Uint32(src[16:20]),
		Seed:                  binary.LittleEndian.Uint64(src[20:28]),
	}
}

func writeUint32s(writer io.Writer, values []uint32, buffer []byte) error {
	for offset := 0; offset < len(values); {
		n := min(len(buffer)/4, len(values)-offset)
		for i := 0; i < n; i++ {
			binary.LittleEndian.PutUint32(buffer[i*4:], values[offset+i])
		}
		if err := writeAll(writer, buffer[:n*4]); err != nil {
			return err
		}
		offset += n
	}
	return nil
}

func readUint32s(data []byte, offset int, values []uint32) (int, error) {
	end := offset + len(values)*4
	if offset < 0 || end < offset || end > len(data) {
		return 0, ErrCorrupt
	}
	for i := range values {
		values[i] = binary.LittleEndian.Uint32(data[offset+i*4:])
	}
	return end, nil
}
