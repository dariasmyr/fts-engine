// Package format encodes and decodes the versioned VHNG graph file format for
// the owning HNSW package.
// It deliberately contains no HNSW implementation dependencies: callers use
// the public DTOs to translate their in-memory graph representation.
package format

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	internalformat "github.com/dariasmyr/fts-engine/internal/format"
)

const (
	Magic         = "VHNG"
	Version       = uint16(1)
	HeaderSize    = 160
	FooterSize    = internalformat.CRC32Size
	maxReadBuffer = 64 << 10
)

var (
	ErrCorruptData        = errors.New("vector/hnsw: corrupt graph data")
	ErrUnsupportedVersion = errors.New("vector/hnsw: unsupported graph format version")
	ErrLimitExceeded      = errors.New("vector/hnsw: graph data exceeds configured limit")
	ErrReferenceMismatch  = errors.New("vector/hnsw: vector file reference mismatch")
)

// Reference identifies the authoritative vector file associated with a graph.
type Reference struct {
	Size   uint64
	SHA256 [sha256.Size]byte
}

// Metadata describes an encoded VHNG file.
type Metadata struct {
	Size   uint64
	CRC32  uint32
	SHA256 [sha256.Size]byte
}

// SearchConfig is the persisted request-search configuration.
type SearchConfig struct {
	DefaultEfSearch, MaxEfSearch           uint32
	DefaultVisitLimit, MaxVisitLimit, MaxK uint32
}

// BuildInfo is the persisted graph-construction provenance.
type BuildInfo struct {
	BuildVersion, LevelGeneratorVersion uint32
	MaxNeighbors, LevelZeroMaxNeighbors uint32
	EfConstruction                      uint32
	Seed                                uint64
}

// Graph is the wire-level VHNG DTO. All topology arrays use graph-local node
// ordinals and are encoded exactly as supplied.
type Graph struct {
	Dimensions       uint32
	Metric           uint8
	Normalization    uint8
	HasEntry         bool
	Entry            uint32
	MaxLevel         uint8
	Search           SearchConfig
	Build            BuildInfo
	Vectors          Reference
	NodeToVector     []uint32
	Levels           []uint8
	Level0Offsets    []uint32
	Level0Links      []uint32
	UpperNodeOffsets []uint32
	UpperLinkOffsets []uint32
	UpperLinks       []uint32
}

// Limits bounds decoding. Zero values select safe defaults.
type Limits struct {
	MaxGraphBytes, MaxVectors, MaxLinks uint64
	MaxDimensions, MaxLevel             uint64
}

func DefaultLimits() Limits {
	return Limits{MaxGraphBytes: 1 << 30, MaxVectors: 10_000_000, MaxLinks: 100_000_000, MaxDimensions: 65_536, MaxLevel: 63}
}

// Encode writes one canonical VHNG file and returns its metadata.
func Encode(writer io.Writer, graph Graph) (Metadata, error) {
	if writer == nil || !validReference(graph.Vectors) || graph.Dimensions == 0 || len(graph.NodeToVector) >= math.MaxUint32 ||
		len(graph.Levels) != len(graph.NodeToVector) || len(graph.Level0Offsets) != len(graph.NodeToVector)+1 ||
		len(graph.UpperNodeOffsets) != len(graph.NodeToVector)+1 || len(graph.UpperLinkOffsets) == 0 ||
		uint64(len(graph.Level0Links)) >= math.MaxUint32 || uint64(len(graph.UpperLinkOffsets)-1) >= math.MaxUint32 || uint64(len(graph.UpperLinks)) >= math.MaxUint32 {
		return Metadata{}, ErrCorruptData
	}
	if !graph.HasEntry && (graph.Entry != 0 || len(graph.NodeToVector) != 0) || graph.HasEntry && uint64(graph.Entry) >= uint64(len(graph.NodeToVector)) {
		return Metadata{}, ErrCorruptData
	}
	if graph.Build.MaxNeighbors == 0 || graph.Build.LevelZeroMaxNeighbors == 0 || graph.Build.EfConstruction == 0 {
		return Metadata{}, ErrCorruptData
	}
	topologyBytes, ok := topologyBytes(uint64(len(graph.NodeToVector)), uint64(len(graph.Level0Links)), uint64(len(graph.UpperLinkOffsets)-1), uint64(len(graph.UpperLinks)))
	if !ok {
		return Metadata{}, ErrLimitExceeded
	}
	fileSize, ok := checkedAdd(HeaderSize+FooterSize, topologyBytes)
	if !ok {
		return Metadata{}, ErrLimitExceeded
	}
	header := make([]byte, HeaderSize)
	copy(header[:4], Magic)
	binary.LittleEndian.PutUint16(header[4:6], Version)
	binary.LittleEndian.PutUint16(header[6:8], HeaderSize)
	binary.LittleEndian.PutUint32(header[8:12], graph.Dimensions)
	header[12], header[13] = graph.Metric, graph.Normalization
	if graph.HasEntry {
		header[14] = 1
	}
	binary.LittleEndian.PutUint32(header[16:20], uint32(len(graph.NodeToVector)))
	binary.LittleEndian.PutUint32(header[20:24], graph.Entry)
	putSearchConfig(header[24:44], graph.Search)
	putBuildInfo(header[44:72], graph.Build)
	binary.LittleEndian.PutUint64(header[72:80], graph.Vectors.Size)
	copy(header[80:112], graph.Vectors.SHA256[:])
	binary.LittleEndian.PutUint32(header[112:116], uint32(len(graph.Level0Links)))
	binary.LittleEndian.PutUint32(header[116:120], uint32(len(graph.UpperLinkOffsets)-1))
	binary.LittleEndian.PutUint32(header[120:124], uint32(len(graph.UpperLinks)))
	maxLevel := graph.MaxLevel
	if len(graph.NodeToVector) == 0 {
		maxLevel = 0xff
	}
	header[124] = maxLevel
	binary.LittleEndian.PutUint64(header[128:136], topologyBytes)
	binary.LittleEndian.PutUint64(header[136:144], fileSize)
	crc, identity := crc32.NewIEEE(), sha256.New()
	body := io.MultiWriter(writer, crc, identity)
	if err := writeBytes(body, header); err != nil {
		return Metadata{}, fmt.Errorf("vector/hnsw: write graph header: %w", err)
	}
	if err := writeUint32s(body, graph.NodeToVector); err != nil {
		return Metadata{}, fmt.Errorf("vector/hnsw: write node mapping: %w", err)
	}
	if err := writeBytes(body, graph.Levels); err != nil {
		return Metadata{}, fmt.Errorf("vector/hnsw: write node levels: %w", err)
	}
	if padding := int(aligned4(uint64(len(graph.Levels)))) - len(graph.Levels); padding != 0 {
		if err := writeBytes(body, make([]byte, padding)); err != nil {
			return Metadata{}, fmt.Errorf("vector/hnsw: write level padding: %w", err)
		}
	}
	for _, section := range [][]uint32{graph.Level0Offsets, graph.Level0Links, graph.UpperNodeOffsets, graph.UpperLinkOffsets, graph.UpperLinks} {
		if err := writeUint32s(body, section); err != nil {
			return Metadata{}, fmt.Errorf("vector/hnsw: write graph topology: %w", err)
		}
	}
	checksum := crc.Sum32()
	var footer [FooterSize]byte
	internalformat.PutCRC32(footer[:], checksum)
	if err := writeBytes(io.MultiWriter(writer, identity), footer[:]); err != nil {
		return Metadata{}, fmt.Errorf("vector/hnsw: write graph checksum: %w", err)
	}
	var digest [sha256.Size]byte
	copy(digest[:], identity.Sum(nil))
	return Metadata{Size: fileSize, CRC32: checksum, SHA256: digest}, nil
}

// Decode reads and validates the VHNG container, but leaves HNSW semantic
// validation (links, levels, and vector source rows) to the owning package.
func Decode(source io.Reader, limits Limits) (Graph, Metadata, error) {
	return DecodeContext(context.Background(), source, limits)
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
	data, err := io.ReadAll(io.LimitReader(contextReader{ctx, source}, int64(limits.MaxGraphBytes)+1))
	if err != nil {
		return Graph{}, Metadata{}, fmt.Errorf("vector/hnsw: read graph: %w", err)
	}
	if uint64(len(data)) > limits.MaxGraphBytes {
		return Graph{}, Metadata{}, ErrLimitExceeded
	}
	return decodeBytes(ctx, data, limits)
}

func decodeBytes(ctx context.Context, data []byte, limits Limits) (Graph, Metadata, error) {
	if len(data) < HeaderSize+FooterSize || string(data[:4]) != Magic {
		return Graph{}, Metadata{}, ErrCorruptData
	}
	if binary.LittleEndian.Uint16(data[4:6]) != Version {
		return Graph{}, Metadata{}, ErrUnsupportedVersion
	}
	if binary.LittleEndian.Uint16(data[6:8]) != HeaderSize || data[15] != 0 || !zero(data[125:128]) || !zero(data[144:160]) {
		return Graph{}, Metadata{}, ErrCorruptData
	}
	checksum, ok := internalformat.ReadCRC32(data[len(data)-FooterSize:])
	if !ok {
		return Graph{}, Metadata{}, ErrCorruptData
	}
	crc, hash := crc32.NewIEEE(), sha256.New()
	for offset := 0; offset < len(data); offset += maxReadBuffer {
		if err := ctx.Err(); err != nil {
			return Graph{}, Metadata{}, err
		}
		end := min(offset+maxReadBuffer, len(data))
		_, _ = hash.Write(data[offset:end])
		if offset < len(data)-FooterSize {
			_, _ = crc.Write(data[offset:min(end, len(data)-FooterSize)])
		}
	}
	if checksum != crc.Sum32() {
		return Graph{}, Metadata{}, ErrCorruptData
	}
	graph := Graph{Dimensions: binary.LittleEndian.Uint32(data[8:12]), Metric: data[12], Normalization: data[13], HasEntry: data[14] == 1, Entry: binary.LittleEndian.Uint32(data[20:24]), MaxLevel: data[124], Vectors: Reference{Size: binary.LittleEndian.Uint64(data[72:80])}}
	copy(graph.Vectors.SHA256[:], data[80:112])
	if !validReference(graph.Vectors) {
		return Graph{}, Metadata{}, ErrCorruptData
	}
	graph.Search = getSearchConfig(data[24:44])
	graph.Build = getBuildInfo(data[44:72])
	nodes, level0, placements, upper := uint64(binary.LittleEndian.Uint32(data[16:20])), uint64(binary.LittleEndian.Uint32(data[112:116])), uint64(binary.LittleEndian.Uint32(data[116:120])), uint64(binary.LittleEndian.Uint32(data[120:124]))
	if nodes > limits.MaxVectors || graph.Dimensions == 0 || uint64(graph.Dimensions) > limits.MaxDimensions || level0+upper > limits.MaxLinks {
		return Graph{}, Metadata{}, ErrLimitExceeded
	}
	topo, ok := topologyBytes(nodes, level0, placements, upper)
	if !ok || uint64(binary.LittleEndian.Uint64(data[128:136])) != topo || uint64(binary.LittleEndian.Uint64(data[136:144])) != uint64(len(data)) {
		return Graph{}, Metadata{}, ErrCorruptData
	}
	graph.Levels = make([]uint8, nodes)
	graph.NodeToVector = make([]uint32, nodes)
	graph.Level0Offsets = make([]uint32, nodes+1)
	graph.Level0Links = make([]uint32, level0)
	graph.UpperNodeOffsets = make([]uint32, nodes+1)
	graph.UpperLinkOffsets = make([]uint32, placements+1)
	graph.UpperLinks = make([]uint32, upper)
	offset := HeaderSize
	var err error
	offset, err = readUint32s(data, offset, graph.NodeToVector)
	if err != nil {
		return Graph{}, Metadata{}, err
	}
	copy(graph.Levels, data[offset:offset+int(nodes)])
	offset += int(aligned4(nodes))
	if !zero(data[offset-int(aligned4(nodes)-nodes) : offset]) {
		return Graph{}, Metadata{}, ErrCorruptData
	}
	for _, section := range []*[]uint32{&graph.Level0Offsets, &graph.Level0Links, &graph.UpperNodeOffsets, &graph.UpperLinkOffsets, &graph.UpperLinks} {
		offset, err = readUint32s(data, offset, *section)
		if err != nil {
			return Graph{}, Metadata{}, err
		}
	}
	if offset != len(data)-FooterSize {
		return Graph{}, Metadata{}, ErrCorruptData
	}
	if data[14] > 1 || (data[124] != 0xff && uint64(data[124]) > 63) {
		return Graph{}, Metadata{}, ErrCorruptData
	}
	return graph, Metadata{Size: uint64(len(data)), CRC32: checksum, SHA256: sha256.Sum256(data)}, nil
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
func validReference(r Reference) bool { return r.Size != 0 && r.SHA256 != [sha256.Size]byte{} }
func topologyBytes(nodes, level0, placements, upper uint64) (uint64, bool) {
	words, ok := checkedAdd(nodes, nodes+1)
	if !ok {
		return 0, false
	}
	for _, n := range []uint64{level0, nodes + 1, placements + 1, upper} {
		words, ok = checkedAdd(words, n)
		if !ok {
			return 0, false
		}
	}
	bytes, ok := checkedMul(words, 4)
	if !ok {
		return 0, false
	}
	return checkedAdd(bytes, aligned4(nodes))
}
func aligned4(v uint64) uint64 { return (v + 3) &^ 3 }
func checkedAdd(a, b uint64) (uint64, bool) {
	if b > math.MaxUint64-a {
		return 0, false
	}
	return a + b, true
}
func checkedMul(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}
func zero(data []byte) bool {
	for _, v := range data {
		if v != 0 {
			return false
		}
	}
	return true
}
func putSearchConfig(dst []byte, c SearchConfig) {
	for i, v := range []uint32{c.DefaultEfSearch, c.MaxEfSearch, c.DefaultVisitLimit, c.MaxVisitLimit, c.MaxK} {
		binary.LittleEndian.PutUint32(dst[i*4:], v)
	}
}
func getSearchConfig(src []byte) SearchConfig {
	return SearchConfig{binary.LittleEndian.Uint32(src[0:4]), binary.LittleEndian.Uint32(src[4:8]), binary.LittleEndian.Uint32(src[8:12]), binary.LittleEndian.Uint32(src[12:16]), binary.LittleEndian.Uint32(src[16:20])}
}
func putBuildInfo(dst []byte, i BuildInfo) {
	binary.LittleEndian.PutUint32(dst[0:], i.BuildVersion)
	binary.LittleEndian.PutUint32(dst[4:], i.LevelGeneratorVersion)
	binary.LittleEndian.PutUint32(dst[8:], i.MaxNeighbors)
	binary.LittleEndian.PutUint32(dst[12:], i.LevelZeroMaxNeighbors)
	binary.LittleEndian.PutUint32(dst[16:], i.EfConstruction)
	binary.LittleEndian.PutUint64(dst[20:], i.Seed)
}
func getBuildInfo(src []byte) BuildInfo {
	return BuildInfo{binary.LittleEndian.Uint32(src), binary.LittleEndian.Uint32(src[4:]), binary.LittleEndian.Uint32(src[8:]), binary.LittleEndian.Uint32(src[12:]), binary.LittleEndian.Uint32(src[16:]), binary.LittleEndian.Uint64(src[20:])}
}
func writeBytes(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
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
func writeUint32s(w io.Writer, values []uint32) error {
	b := make([]byte, 64<<10)
	for off := 0; off < len(values); {
		n := min(len(b)/4, len(values)-off)
		for i := 0; i < n; i++ {
			binary.LittleEndian.PutUint32(b[i*4:], values[off+i])
		}
		if err := writeBytes(w, b[:n*4]); err != nil {
			return err
		}
		off += n
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
