package hnsw

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
	graphFormatMagic = "VHNG"
	// GraphFormatVersion identifies the persisted graph.bin layout.
	GraphFormatVersion    = uint16(1)
	graphFormatHeaderSize = 160
	graphFormatFooterSize = 4
)

var (
	ErrCorruptGraphData        = errors.New("vector/hnsw: corrupt graph data")
	ErrUnsupportedGraphVersion = errors.New("vector/hnsw: unsupported graph format version")
	ErrGraphLimitExceeded      = errors.New("vector/hnsw: graph data exceeds configured limit")
	ErrVectorFileRefMismatch   = errors.New("vector/hnsw: vector file reference mismatch")
	ErrGraphVectorSource       = errors.New("vector/hnsw: graph vector source mismatch")
)

// GraphLimits bounds both graph decoding and the prepared vector matrix copied
// into an opened reader. Zero fields select the corresponding default.
type GraphLimits struct {
	MaxDimensions     int
	MaxVectors        int
	MaxVectorBytes    uint64
	MaxGraphBytes     uint64
	MaxLinks          uint64
	MaxLevel          int
	MaxEfSearch       int
	MaxVisitLimit     int
	MaxK              int
	MaxNeighbors      int
	MaxEfConstruction int
}

// DefaultGraphLimits returns safe default bounds for opening graph data.
func DefaultGraphLimits() GraphLimits {
	return GraphLimits{
		MaxDimensions: 65_536, MaxVectors: 10_000_000,
		MaxVectorBytes: 512 << 20, MaxGraphBytes: 1 << 30,
		MaxLinks: 100_000_000, MaxLevel: MaxLevel,
		MaxEfSearch: 1_000_000, MaxVisitLimit: 10_000_000, MaxK: 1_000_000,
		MaxNeighbors: MaxSupportedNeighbors, MaxEfConstruction: MaxEfConstruction,
	}
}

// VectorFileReference identifies the authoritative, separately persisted
// vector file used by a graph. CRC32 is deliberately excluded: SHA256 is the
// durable identity and Size rejects substitution or truncation.
type VectorFileReference struct {
	Size   uint64
	SHA256 [sha256.Size]byte
}

// FileMetadata describes the complete encoded graph file.
type FileMetadata struct {
	Size   uint64
	CRC32  uint32
	SHA256 [sha256.Size]byte
}

func MarshalGraph(reader *Reader, vectors VectorFileReference) ([]byte, FileMetadata, error) {
	var buffer bytes.Buffer
	metadata, err := WriteGraph(&buffer, reader, vectors)
	return buffer.Bytes(), metadata, err
}

// WriteGraph writes exact packed topology and configuration. Vector values are
// not included; vectors identifies their authoritative separate file.
func WriteGraph(writer io.Writer, reader *Reader, vectors VectorFileReference) (FileMetadata, error) {
	if writer == nil || reader == nil || !validVectorFileReference(vectors) {
		return FileMetadata{}, ErrCorruptGraphData
	}
	stats, err := validatePackedTopology(reader)
	if err != nil {
		return FileMetadata{}, fmt.Errorf("%w: %v", ErrCorruptGraphData, err)
	}
	if err := reader.searchConfig.validate(); err != nil {
		return FileMetadata{}, fmt.Errorf("%w: %v", ErrCorruptGraphData, err)
	}
	if err := validatePersistedBuildInfo(reader.buildInfo); err != nil {
		return FileMetadata{}, fmt.Errorf("%w: %v", ErrCorruptGraphData, err)
	}
	if !readerConfigEncodable(reader) {
		return FileMetadata{}, ErrGraphLimitExceeded
	}
	if err := validateReaderVectors(reader); err != nil {
		return FileMetadata{}, err
	}

	nodes := uint64(reader.Len())
	level0Links := uint64(len(reader.level0Neighbors))
	upperPlacements := uint64(len(reader.upperLinkOffsets) - 1)
	upperLinks := uint64(len(reader.upperNeighbors))
	topologyBytes, ok := graphTopologyBytes(nodes, level0Links, upperPlacements, upperLinks)
	if !ok || nodes >= math.MaxUint32 || level0Links >= math.MaxUint32 || upperPlacements >= math.MaxUint32 || upperLinks >= math.MaxUint32 {
		return FileMetadata{}, ErrGraphLimitExceeded
	}
	fileSize, ok := checkedAdd(uint64(graphFormatHeaderSize+graphFormatFooterSize), topologyBytes)
	if !ok {
		return FileMetadata{}, ErrGraphLimitExceeded
	}

	header := make([]byte, graphFormatHeaderSize)
	copy(header[:4], graphFormatMagic)
	binary.LittleEndian.PutUint16(header[4:6], GraphFormatVersion)
	binary.LittleEndian.PutUint16(header[6:8], graphFormatHeaderSize)
	binary.LittleEndian.PutUint32(header[8:12], uint32(reader.Dimensions()))
	header[12] = byte(reader.Metric())
	header[13] = byte(reader.Normalization())
	if reader.hasEntry {
		header[14] = 1
	}
	binary.LittleEndian.PutUint32(header[16:20], uint32(nodes))
	binary.LittleEndian.PutUint32(header[20:24], uint32(reader.entry))
	putSearchConfig(header[24:44], reader.searchConfig)
	putBuildInfo(header[44:72], reader.buildInfo)
	binary.LittleEndian.PutUint64(header[72:80], vectors.Size)
	copy(header[80:112], vectors.SHA256[:])
	binary.LittleEndian.PutUint32(header[112:116], uint32(level0Links))
	binary.LittleEndian.PutUint32(header[116:120], uint32(upperPlacements))
	binary.LittleEndian.PutUint32(header[120:124], uint32(upperLinks))
	if stats.MaxLevel < 0 {
		header[124] = 0xff
	} else {
		header[124] = byte(stats.MaxLevel)
	}
	binary.LittleEndian.PutUint64(header[128:136], topologyBytes)
	binary.LittleEndian.PutUint64(header[136:144], fileSize)

	crc := crc32.NewIEEE()
	identity := sha256.New()
	body := io.MultiWriter(writer, crc, identity)
	if err := writeGraphBytes(body, header); err != nil {
		return FileMetadata{}, fmt.Errorf("vector/hnsw: write graph header: %w", err)
	}
	if err := writeGraphUint32s(body, reader.nodeToVector); err != nil {
		return FileMetadata{}, fmt.Errorf("vector/hnsw: write node mapping: %w", err)
	}
	if err := writeGraphBytes(body, reader.levels); err != nil {
		return FileMetadata{}, fmt.Errorf("vector/hnsw: write node levels: %w", err)
	}
	if padding := aligned4(nodes) - nodes; padding != 0 {
		if err := writeGraphBytes(body, make([]byte, padding)); err != nil {
			return FileMetadata{}, fmt.Errorf("vector/hnsw: write level padding: %w", err)
		}
	}
	if err := writeGraphUint32s(body, reader.level0Offsets); err != nil {
		return FileMetadata{}, fmt.Errorf("vector/hnsw: write level-0 offsets: %w", err)
	}
	if err := writeGraphUint32s(body, reader.level0Neighbors); err != nil {
		return FileMetadata{}, fmt.Errorf("vector/hnsw: write level-0 links: %w", err)
	}
	if err := writeGraphUint32s(body, reader.upperNodeOffsets); err != nil {
		return FileMetadata{}, fmt.Errorf("vector/hnsw: write upper-node offsets: %w", err)
	}
	if err := writeGraphUint32s(body, reader.upperLinkOffsets); err != nil {
		return FileMetadata{}, fmt.Errorf("vector/hnsw: write upper-link offsets: %w", err)
	}
	if err := writeGraphUint32s(body, reader.upperNeighbors); err != nil {
		return FileMetadata{}, fmt.Errorf("vector/hnsw: write upper links: %w", err)
	}

	checksum := crc.Sum32()
	var footer [graphFormatFooterSize]byte
	binary.LittleEndian.PutUint32(footer[:], checksum)
	if err := writeGraphBytes(io.MultiWriter(writer, identity), footer[:]); err != nil {
		return FileMetadata{}, fmt.Errorf("vector/hnsw: write graph checksum: %w", err)
	}
	metadata := FileMetadata{Size: fileSize, CRC32: checksum}
	copy(metadata.SHA256[:], identity.Sum(nil))
	return metadata, nil
}

// OpenIndexReader validates a graph file and its vector-file binding, then
// returns an immutable index reader over decoded graph sections backed by vectors.
func OpenIndexReader(source io.Reader, vectors vector.PreparedVectorSource, vectorFile VectorFileReference, limits GraphLimits) (*Reader, FileMetadata, error) {
	return OpenIndexReaderContext(context.Background(), source, vectors, vectorFile, limits)
}

// OpenIndexReaderContext is OpenIndexReader with cancellation for reads,
// decoding, validation, and vector access.
func OpenIndexReaderContext(ctx context.Context, source io.Reader, vectors vector.PreparedVectorSource, vectorFile VectorFileReference, limits GraphLimits) (*Reader, FileMetadata, error) {
	if ctx == nil {
		return nil, FileMetadata{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, FileMetadata{}, err
	}
	if source == nil || vectors == nil || isNilPreparedVectorSource(vectors) {
		return nil, FileMetadata{}, ErrGraphVectorSource
	}
	if !validVectorFileReference(vectorFile) {
		return nil, FileMetadata{}, ErrVectorFileRefMismatch
	}
	limits = normalizeGraphLimits(limits)
	if err := validateGraphLimits(limits); err != nil {
		return nil, FileMetadata{}, err
	}
	if limits.MaxGraphBytes >= math.MaxInt64 {
		return nil, FileMetadata{}, ErrGraphLimitExceeded
	}
	data, err := io.ReadAll(io.LimitReader(contextGraphReader{ctx: ctx, reader: source}, int64(limits.MaxGraphBytes)+1))
	if err != nil {
		return nil, FileMetadata{}, fmt.Errorf("vector/hnsw: read graph: %w", err)
	}
	if uint64(len(data)) > limits.MaxGraphBytes {
		return nil, FileMetadata{}, ErrGraphLimitExceeded
	}
	return openGraphBytes(ctx, data, vectors, vectorFile, limits)
}

func openGraphBytes(ctx context.Context, data []byte, vectors vector.PreparedVectorSource, vectorFile VectorFileReference, limits GraphLimits) (*Reader, FileMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, FileMetadata{}, err
	}
	if len(data) < graphFormatHeaderSize+graphFormatFooterSize || string(data[:4]) != graphFormatMagic {
		return nil, FileMetadata{}, ErrCorruptGraphData
	}
	if binary.LittleEndian.Uint16(data[4:6]) != GraphFormatVersion {
		return nil, FileMetadata{}, ErrUnsupportedGraphVersion
	}
	if binary.LittleEndian.Uint16(data[6:8]) != graphFormatHeaderSize || data[15] != 0 || !allZero(data[125:128]) || !allZero(data[144:160]) {
		return nil, FileMetadata{}, ErrCorruptGraphData
	}
	checksum := binary.LittleEndian.Uint32(data[len(data)-graphFormatFooterSize:])
	actualChecksum, identity, err := graphFileMetadataContext(ctx, data)
	if err != nil {
		return nil, FileMetadata{}, err
	}
	if actualChecksum != checksum {
		return nil, FileMetadata{}, ErrCorruptGraphData
	}
	storedReference := VectorFileReference{Size: binary.LittleEndian.Uint64(data[72:80])}
	copy(storedReference.SHA256[:], data[80:112])
	if !validVectorFileReference(storedReference) {
		return nil, FileMetadata{}, ErrCorruptGraphData
	}
	if storedReference != vectorFile {
		return nil, FileMetadata{}, ErrVectorFileRefMismatch
	}

	dimensions64 := uint64(binary.LittleEndian.Uint32(data[8:12]))
	nodes := uint64(binary.LittleEndian.Uint32(data[16:20]))
	level0Links := uint64(binary.LittleEndian.Uint32(data[112:116]))
	upperPlacements := uint64(binary.LittleEndian.Uint32(data[116:120]))
	upperLinks := uint64(binary.LittleEndian.Uint32(data[120:124]))
	if dimensions64 == 0 || dimensions64 > uint64(limits.MaxDimensions) || dimensions64 > uint64(math.MaxInt) ||
		nodes > uint64(limits.MaxVectors) || nodes > uint64(math.MaxInt) {
		return nil, FileMetadata{}, ErrGraphLimitExceeded
	}
	totalLinks, ok := checkedAdd(level0Links, upperLinks)
	if !ok || totalLinks > limits.MaxLinks || totalLinks > uint64(math.MaxInt) || upperPlacements > uint64(math.MaxInt) || level0Links > uint64(math.MaxInt) || upperLinks > uint64(math.MaxInt) {
		return nil, FileMetadata{}, ErrGraphLimitExceeded
	}
	maxPlacements, ok := checkedMultiply(nodes, uint64(limits.MaxLevel))
	if !ok || upperPlacements > maxPlacements {
		return nil, FileMetadata{}, ErrCorruptGraphData
	}
	topologyBytes, ok := graphTopologyBytes(nodes, level0Links, upperPlacements, upperLinks)
	fileSize, sizeOK := checkedAdd(uint64(graphFormatHeaderSize+graphFormatFooterSize), topologyBytes)
	if !ok || !sizeOK || binary.LittleEndian.Uint64(data[128:136]) != topologyBytes ||
		binary.LittleEndian.Uint64(data[136:144]) != fileSize || fileSize != uint64(len(data)) {
		return nil, FileMetadata{}, ErrCorruptGraphData
	}
	components, ok := checkedMultiply(nodes, dimensions64)
	vectorBytes, bytesOK := checkedMultiply(components, 4)
	if !ok || !bytesOK || components > uint64(math.MaxInt) || vectorBytes > limits.MaxVectorBytes {
		return nil, FileMetadata{}, ErrGraphLimitExceeded
	}

	dimensions := int(dimensions64)
	metric := vector.Metric(data[12])
	normalization := vector.Normalization(data[13])
	space, err := vector.NewSpace(dimensions, metric)
	if err != nil || space.Normalization() != normalization {
		return nil, FileMetadata{}, ErrCorruptGraphData
	}
	searchConfig, ok := getSearchConfig(data[24:44])
	if !ok || searchConfig.DefaultEfSearch > limits.MaxEfSearch || searchConfig.MaxEfSearch > limits.MaxEfSearch ||
		searchConfig.DefaultVisitLimit > limits.MaxVisitLimit || searchConfig.MaxVisitLimit > limits.MaxVisitLimit || searchConfig.MaxK > limits.MaxK {
		return nil, FileMetadata{}, ErrGraphLimitExceeded
	}
	if err := searchConfig.validate(); err != nil {
		return nil, FileMetadata{}, ErrCorruptGraphData
	}
	buildInfo, ok := getBuildInfo(data[44:72])
	if !ok || buildInfo.MaxNeighbors > limits.MaxNeighbors || buildInfo.LevelZeroMaxNeighbors > limits.MaxNeighbors*2 || buildInfo.EfConstruction > limits.MaxEfConstruction {
		return nil, FileMetadata{}, ErrGraphLimitExceeded
	}
	if err := validatePersistedBuildInfo(buildInfo); err != nil {
		return nil, FileMetadata{}, ErrCorruptGraphData
	}
	if data[124] != 0xff && int(data[124]) > limits.MaxLevel {
		return nil, FileMetadata{}, ErrGraphLimitExceeded
	}

	reader := &Reader{
		topology: topology{
			space: space, searchConfig: searchConfig, buildInfo: buildInfo,
			nodeToVector: make([]vector.Ordinal, int(nodes)), levels: make([]uint8, int(nodes)),
			level0Offsets: make([]uint32, int(nodes)+1), level0Neighbors: make([]NodeOrdinal, int(level0Links)),
			upperNodeOffsets: make([]uint32, int(nodes)+1), upperLinkOffsets: make([]uint32, int(upperPlacements)+1),
			upperNeighbors: make([]NodeOrdinal, int(upperLinks)),
		},
		source: vectors,
	}
	offset := graphFormatHeaderSize
	offset, err = readGraphUint32sContext(ctx, data, offset, reader.nodeToVector)
	if err != nil {
		return nil, FileMetadata{}, err
	}
	copy(reader.levels, data[offset:offset+int(nodes)])
	offset += int(aligned4(nodes))
	if !allZero(data[offset-int(aligned4(nodes)-nodes) : offset]) {
		return nil, FileMetadata{}, ErrCorruptGraphData
	}
	offset, err = readGraphUint32sContext(ctx, data, offset, reader.level0Offsets)
	if err != nil {
		return nil, FileMetadata{}, err
	}
	offset, err = readGraphUint32sContext(ctx, data, offset, reader.level0Neighbors)
	if err != nil {
		return nil, FileMetadata{}, err
	}
	offset, err = readGraphUint32sContext(ctx, data, offset, reader.upperNodeOffsets)
	if err != nil {
		return nil, FileMetadata{}, err
	}
	offset, err = readGraphUint32sContext(ctx, data, offset, reader.upperLinkOffsets)
	if err != nil {
		return nil, FileMetadata{}, err
	}
	offset, err = readGraphUint32sContext(ctx, data, offset, reader.upperNeighbors)
	if err != nil {
		return nil, FileMetadata{}, err
	}
	if offset != len(data)-graphFormatFooterSize {
		return nil, FileMetadata{}, ErrCorruptGraphData
	}
	reader.hasEntry = data[14] == 1
	if data[14] > 1 {
		return nil, FileMetadata{}, ErrCorruptGraphData
	}
	reader.entry = NodeOrdinal(binary.LittleEndian.Uint32(data[20:24]))
	stats, err := validatePackedTopologyContext(ctx, reader)
	if err != nil && ctx.Err() != nil {
		return nil, FileMetadata{}, ctx.Err()
	}
	if err != nil || (stats.MaxLevel < 0 && data[124] != 0xff) || (stats.MaxLevel >= 0 && data[124] != byte(stats.MaxLevel)) {
		return nil, FileMetadata{}, ErrCorruptGraphData
	}
	if stats.MaxLevel > limits.MaxLevel {
		return nil, FileMetadata{}, ErrGraphLimitExceeded
	}

	if vectors.Len() != int(nodes) || vectors.Dimensions() != dimensions || vectors.Metric() != metric || vectors.Normalization() != normalization {
		return nil, FileMetadata{}, ErrGraphVectorSource
	}
	if err := validateReaderVectorsContext(ctx, reader); err != nil {
		return nil, FileMetadata{}, err
	}
	reader.stats = cloneGraphStats(stats)
	reader.validated = true
	if err := ctx.Err(); err != nil {
		return nil, FileMetadata{}, err
	}
	return reader, FileMetadata{Size: uint64(len(data)), CRC32: checksum, SHA256: identity}, nil
}

func graphFileMetadataContext(ctx context.Context, data []byte) (uint32, [sha256.Size]byte, error) {
	crc := crc32.NewIEEE()
	identity := sha256.New()
	bodyBytes := len(data) - graphFormatFooterSize
	for offset := 0; offset < len(data); {
		if err := ctx.Err(); err != nil {
			return 0, [sha256.Size]byte{}, err
		}
		end := min(offset+(64<<10), len(data))
		_, _ = identity.Write(data[offset:end])
		if offset < bodyBytes {
			_, _ = crc.Write(data[offset:min(end, bodyBytes)])
		}
		offset = end
	}
	if err := ctx.Err(); err != nil {
		return 0, [sha256.Size]byte{}, err
	}
	var digest [sha256.Size]byte
	copy(digest[:], identity.Sum(nil))
	return crc.Sum32(), digest, nil
}

type contextGraphReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextGraphReader) Read(dst []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(dst)
	if err == nil {
		err = r.ctx.Err()
	}
	return n, err
}

func validateReaderVectors(reader *Reader) error {
	return validateReaderVectorsContext(context.Background(), reader)
}

func validateReaderVectorsContext(ctx context.Context, reader *Reader) error {
	if reader == nil || reader.source == nil || reader.source.Len() != reader.Len() ||
		reader.source.Dimensions() != reader.Dimensions() || reader.source.Metric() != reader.Metric() || reader.source.Normalization() != reader.Normalization() {
		return ErrCorruptGraphData
	}
	components, ok := checkedMultiply(uint64(reader.Len()), uint64(reader.Dimensions()))
	if !ok || components > uint64(math.MaxInt) {
		return ErrCorruptGraphData
	}
	scratch := make([]float32, reader.Dimensions())
	for row := range reader.Len() {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i := range scratch {
			scratch[i] = float32(math.NaN())
		}
		if err := reader.source.ReadVectorInto(ctx, vector.Ordinal(row), scratch); err != nil {
			return fmt.Errorf("%w: read vector row %d: %v", ErrGraphVectorSource, row, err)
		}
		if err := validatePreparedVector(reader.space, scratch); err != nil {
			return fmt.Errorf("%w: vector row %d: %v", ErrGraphVectorSource, row, err)
		}
	}
	return nil
}

func validatePackedTopology(reader *Reader) (GraphStats, error) {
	return validatePackedTopologyContext(context.Background(), reader)
}

func validatePackedTopologyContext(ctx context.Context, reader *Reader) (GraphStats, error) {
	if err := ctx.Err(); err != nil {
		return GraphStats{}, err
	}
	if reader == nil || reader.Dimensions() <= 0 || !reader.Metric().Valid() {
		return GraphStats{}, ErrInvalidGraph
	}
	nodes := len(reader.nodeToVector)
	if uint64(nodes) >= math.MaxUint32 || len(reader.levels) != nodes || len(reader.level0Offsets) != nodes+1 ||
		len(reader.upperNodeOffsets) != nodes+1 || len(reader.upperLinkOffsets) == 0 ||
		uint64(len(reader.level0Neighbors)) >= math.MaxUint32 || uint64(len(reader.upperLinkOffsets)-1) >= math.MaxUint32 || uint64(len(reader.upperNeighbors)) >= math.MaxUint32 {
		return GraphStats{}, ErrInvalidGraph
	}
	if links, ok := checkedAdd(uint64(len(reader.level0Neighbors)), uint64(len(reader.upperNeighbors))); !ok || links > uint64(math.MaxInt) {
		return GraphStats{}, ErrInvalidGraph
	}
	if nodes == 0 {
		if reader.hasEntry || reader.entry != 0 || len(reader.level0Neighbors) != 0 || len(reader.upperLinkOffsets) != 1 || len(reader.upperNeighbors) != 0 ||
			reader.level0Offsets[0] != 0 || reader.upperNodeOffsets[0] != 0 || reader.upperLinkOffsets[0] != 0 {
			return GraphStats{}, ErrInvalidGraph
		}
		return GraphStats{MaxLevel: -1}, nil
	}
	if !reader.hasEntry || uint64(reader.entry) >= uint64(nodes) {
		return GraphStats{}, ErrInvalidGraph
	}
	seenVectors := make([]bool, nodes)
	maxLevel := 0
	placements := uint64(0)
	for node := range nodes {
		if err := contextProgressCheck(ctx, node); err != nil {
			return GraphStats{}, err
		}
		ordinal := reader.nodeToVector[node]
		level := int(reader.levels[node])
		if uint64(ordinal) >= uint64(nodes) || seenVectors[ordinal] || level > MaxLevel || uint64(reader.upperNodeOffsets[node]) != placements {
			return GraphStats{}, ErrInvalidGraph
		}
		seenVectors[ordinal] = true
		placements += uint64(level)
		if placements >= math.MaxUint32 {
			return GraphStats{}, ErrInvalidGraph
		}
		maxLevel = max(maxLevel, level)
	}
	if uint64(reader.upperNodeOffsets[nodes]) != placements || placements != uint64(len(reader.upperLinkOffsets)-1) || int(reader.levels[reader.entry]) != maxLevel {
		return GraphStats{}, ErrInvalidGraph
	}
	if !validOffsetsContext(ctx, reader.level0Offsets, len(reader.level0Neighbors)) || !validOffsetsContext(ctx, reader.upperLinkOffsets, len(reader.upperNeighbors)) {
		if err := ctx.Err(); err != nil {
			return GraphStats{}, err
		}
		return GraphStats{}, ErrInvalidGraph
	}

	stats := GraphStats{
		NodeCount: nodes, VectorCount: nodes, MaxLevel: maxLevel,
		LevelNodeCounts: make([]int, maxLevel+1), LevelLinkCounts: make([]int, maxLevel+1),
	}
	marks := make([]uint64, nodes)
	var epoch uint64
	for node := range nodes {
		if err := contextProgressCheck(ctx, node); err != nil {
			return GraphStats{}, err
		}
		for level := 0; level <= int(reader.levels[node]); level++ {
			neighbors, ok := reader.neighborView(NodeOrdinal(node), level)
			if !ok || len(neighbors) > reader.buildInfo.neighborLimit(level) {
				return GraphStats{}, ErrInvalidGraph
			}
			stats.LevelNodeCounts[level]++
			stats.LevelLinkCounts[level] += len(neighbors)
			epoch++
			for _, neighbor := range neighbors {
				if uint64(neighbor) >= uint64(nodes) || int(neighbor) == node || int(reader.levels[neighbor]) < level || marks[neighbor] == epoch {
					return GraphStats{}, ErrInvalidGraph
				}
				marks[neighbor] = epoch
			}
		}
		if reader.level0Offsets[node] == reader.level0Offsets[node+1] {
			stats.ZeroDegreeNodes++
		}
	}
	visited := make([]bool, nodes)
	queue := make([]NodeOrdinal, 1, nodes)
	queue[0] = reader.entry
	visited[reader.entry] = true
	processed := 0
	for len(queue) > 0 {
		if err := contextProgressCheck(ctx, processed); err != nil {
			return GraphStats{}, err
		}
		node := queue[0]
		queue = queue[1:]
		processed++
		stats.ReachableNodes++
		neighbors, _ := reader.neighborView(node, 0)
		for _, neighbor := range neighbors {
			if !visited[neighbor] {
				visited[neighbor] = true
				queue = append(queue, neighbor)
			}
		}
	}
	stats.UnreachableNodes = nodes - stats.ReachableNodes
	if err := ctx.Err(); err != nil {
		return GraphStats{}, err
	}
	return stats, nil
}

func validOffsetsContext(ctx context.Context, offsets []uint32, values int) bool {
	if len(offsets) == 0 || offsets[0] != 0 || uint64(offsets[len(offsets)-1]) != uint64(values) {
		return false
	}
	for i := 1; i < len(offsets); i++ {
		if contextProgressCheck(ctx, i) != nil {
			return false
		}
		if offsets[i] < offsets[i-1] || uint64(offsets[i]) > uint64(values) {
			return false
		}
	}
	return true
}

func contextProgressCheck(ctx context.Context, index int) error {
	if index&0x3fff != 0 {
		return nil
	}
	return ctx.Err()
}

func readerConfigEncodable(reader *Reader) bool {
	values := [...]int{
		reader.Dimensions(), reader.searchConfig.DefaultEfSearch, reader.searchConfig.MaxEfSearch,
		reader.searchConfig.DefaultVisitLimit, reader.searchConfig.MaxVisitLimit, reader.searchConfig.MaxK,
		reader.buildInfo.MaxNeighbors, reader.buildInfo.LevelZeroMaxNeighbors, reader.buildInfo.EfConstruction,
	}
	for _, value := range values {
		if value < 0 || uint64(value) > math.MaxUint32 {
			return false
		}
	}
	return true
}

func graphTopologyBytes(nodes, level0Links, upperPlacements, upperLinks uint64) (uint64, bool) {
	words, ok := checkedMultiply(nodes, 1) // node-to-vector mapping
	if !ok {
		return 0, false
	}
	for _, count := range []uint64{nodes + 1, level0Links, nodes + 1, upperPlacements + 1, upperLinks} {
		words, ok = checkedAdd(words, count)
		if !ok {
			return 0, false
		}
	}
	wordBytes, ok := checkedMultiply(words, 4)
	if !ok {
		return 0, false
	}
	return checkedAdd(wordBytes, aligned4(nodes))
}

func aligned4(value uint64) uint64 { return (value + 3) &^ 3 }

func putSearchConfig(dst []byte, config SearchConfig) {
	values := [...]int{config.DefaultEfSearch, config.MaxEfSearch, config.DefaultVisitLimit, config.MaxVisitLimit, config.MaxK}
	for i, value := range values {
		binary.LittleEndian.PutUint32(dst[i*4:(i+1)*4], uint32(value))
	}
}

func getSearchConfig(src []byte) (SearchConfig, bool) {
	values := [5]int{}
	for i := range values {
		value := uint64(binary.LittleEndian.Uint32(src[i*4 : (i+1)*4]))
		if value > uint64(math.MaxInt) {
			return SearchConfig{}, false
		}
		values[i] = int(value)
	}
	return SearchConfig{DefaultEfSearch: values[0], MaxEfSearch: values[1], DefaultVisitLimit: values[2], MaxVisitLimit: values[3], MaxK: values[4]}, true
}

func putBuildInfo(dst []byte, info BuildInfo) {
	binary.LittleEndian.PutUint32(dst[0:4], info.BuildVersion)
	binary.LittleEndian.PutUint32(dst[4:8], info.LevelGeneratorVersion)
	binary.LittleEndian.PutUint32(dst[8:12], uint32(info.MaxNeighbors))
	binary.LittleEndian.PutUint32(dst[12:16], uint32(info.LevelZeroMaxNeighbors))
	binary.LittleEndian.PutUint32(dst[16:20], uint32(info.EfConstruction))
	binary.LittleEndian.PutUint64(dst[20:28], info.Seed)
}

func getBuildInfo(src []byte) (BuildInfo, bool) {
	maxNeighbors := uint64(binary.LittleEndian.Uint32(src[8:12]))
	level0Max := uint64(binary.LittleEndian.Uint32(src[12:16]))
	ef := uint64(binary.LittleEndian.Uint32(src[16:20]))
	if maxNeighbors > uint64(math.MaxInt) || level0Max > uint64(math.MaxInt) || ef > uint64(math.MaxInt) {
		return BuildInfo{}, false
	}
	return BuildInfo{
		BuildVersion: binary.LittleEndian.Uint32(src[0:4]), LevelGeneratorVersion: binary.LittleEndian.Uint32(src[4:8]),
		MaxNeighbors: int(maxNeighbors), LevelZeroMaxNeighbors: int(level0Max), EfConstruction: int(ef),
		Seed: binary.LittleEndian.Uint64(src[20:28]),
	}, true
}

func normalizeGraphLimits(limits GraphLimits) GraphLimits {
	defaults := DefaultGraphLimits()
	if limits.MaxDimensions == 0 {
		limits.MaxDimensions = defaults.MaxDimensions
	}
	if limits.MaxVectors == 0 {
		limits.MaxVectors = defaults.MaxVectors
	}
	if limits.MaxVectorBytes == 0 {
		limits.MaxVectorBytes = defaults.MaxVectorBytes
	}
	if limits.MaxGraphBytes == 0 {
		limits.MaxGraphBytes = defaults.MaxGraphBytes
	}
	if limits.MaxLinks == 0 {
		limits.MaxLinks = defaults.MaxLinks
	}
	if limits.MaxLevel == 0 {
		limits.MaxLevel = defaults.MaxLevel
	}
	if limits.MaxEfSearch == 0 {
		limits.MaxEfSearch = defaults.MaxEfSearch
	}
	if limits.MaxVisitLimit == 0 {
		limits.MaxVisitLimit = defaults.MaxVisitLimit
	}
	if limits.MaxK == 0 {
		limits.MaxK = defaults.MaxK
	}
	if limits.MaxNeighbors == 0 {
		limits.MaxNeighbors = defaults.MaxNeighbors
	}
	if limits.MaxEfConstruction == 0 {
		limits.MaxEfConstruction = defaults.MaxEfConstruction
	}
	return limits
}

func validateGraphLimits(limits GraphLimits) error {
	if limits.MaxDimensions <= 0 || limits.MaxVectors <= 0 || limits.MaxVectorBytes == 0 || limits.MaxGraphBytes < graphFormatHeaderSize+graphFormatFooterSize ||
		limits.MaxLinks == 0 || limits.MaxLevel < 0 || limits.MaxLevel > MaxLevel || limits.MaxEfSearch <= 0 || limits.MaxVisitLimit <= 0 ||
		limits.MaxK <= 0 || limits.MaxNeighbors < 2 || limits.MaxNeighbors > MaxSupportedNeighbors || limits.MaxEfConstruction < 2 || limits.MaxEfConstruction > MaxEfConstruction {
		return ErrGraphLimitExceeded
	}
	return nil
}

func validVectorFileReference(reference VectorFileReference) bool {
	return reference.Size != 0 && reference.SHA256 != [sha256.Size]byte{}
}

func allZero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}

func writeGraphBytes(writer io.Writer, data []byte) error {
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

func writeGraphUint32s[T ~uint32](writer io.Writer, values []T) error {
	buffer := make([]byte, 64<<10)
	for offset := 0; offset < len(values); {
		count := min(len(buffer)/4, len(values)-offset)
		encoded := buffer[:count*4]
		for i := range count {
			binary.LittleEndian.PutUint32(encoded[i*4:(i+1)*4], uint32(values[offset+i]))
		}
		if err := writeGraphBytes(writer, encoded); err != nil {
			return err
		}
		offset += count
	}
	return nil
}

func readGraphUint32sContext[T ~uint32](ctx context.Context, data []byte, offset int, values []T) (int, error) {
	for start := 0; start < len(values); start += 16 << 10 {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		end := min(start+(16<<10), len(values))
		for i := start; i < end; i++ {
			values[i] = T(binary.LittleEndian.Uint32(data[offset+i*4 : offset+(i+1)*4]))
		}
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return offset + len(values)*4, nil
}

func checkedAdd(a, b uint64) (uint64, bool) {
	if b > math.MaxUint64-a {
		return 0, false
	}
	return a + b, true
}
