package hnsw

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	vhng "github.com/dariasmyr/fts-engine/pkg/vector/hnsw/internal/format"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

const (
	graphFormatMagic = vhng.Magic
	// GraphFormatVersion identifies the persisted graph.bin layout.
	GraphFormatVersion    = vhng.Version
	graphFormatHeaderSize = vhng.HeaderSize
	graphFormatFooterSize = vhng.FooterSize
)

var (
	ErrCorruptGraphData        = errors.New("vector/hnsw: corrupt graph data")
	ErrUnsupportedGraphVersion = errors.New("vector/hnsw: unsupported graph format version")
	ErrGraphLimitExceeded      = errors.New("vector/hnsw: graph data exceeds configured limit")
	ErrVectorFileRefMismatch   = errors.New("vector/hnsw: vector file reference mismatch")
	ErrGraphVectorStore        = errors.New("vector/hnsw: graph vector store mismatch")
)

// GraphLimits bounds both graph decoding and the prepared vector matrix copied
// into an opened HNSW index. Zero fields select the corresponding default.
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
	return GraphLimits{MaxDimensions: 65_536, MaxVectors: 10_000_000, MaxVectorBytes: 512 << 20, MaxGraphBytes: 1 << 30, MaxLinks: 100_000_000, MaxLevel: MaxLevel, MaxEfSearch: 1_000_000, MaxVisitLimit: 10_000_000, MaxK: 1_000_000, MaxNeighbors: MaxSupportedNeighbors, MaxEfConstruction: MaxEfConstruction}
}

// VectorFileReference identifies the authoritative, separately persisted
// vector file used by a graph. CRC32 is deliberately excluded: SHA256 is the
// durable identity and Size rejects substitution or truncation.
type VectorFileReference struct {
	Size   uint64
	SHA256 [32]byte
}

// FileMetadata describes the complete encoded graph file.
type FileMetadata struct {
	Size   uint64
	CRC32  uint32
	SHA256 [32]byte
}

func MarshalGraph(index *HNSWIndex, vectors VectorFileReference) ([]byte, FileMetadata, error) {
	var buffer bytes.Buffer
	metadata, err := WriteGraphFile(&buffer, index, vectors)
	return buffer.Bytes(), metadata, err
}

// WriteGraphFile writes exact packed topology and configuration. Vector values are
// not included; vectors identifies their authoritative separate file.
func WriteGraphFile(writer io.Writer, index *HNSWIndex, vectors VectorFileReference) (FileMetadata, error) {
	if writer == nil || index == nil || !validVectorFileReference(vectors) {
		return FileMetadata{}, ErrCorruptGraphData
	}
	if _, err := validatePackedTopology(index); err != nil {
		return FileMetadata{}, fmt.Errorf("%w: %v", ErrCorruptGraphData, err)
	}
	if err := index.topology.searchConfig.validate(); err != nil {
		return FileMetadata{}, fmt.Errorf("%w: %v", ErrCorruptGraphData, err)
	}
	if err := validatePersistedBuildInfo(index.topology.buildInfo); err != nil {
		return FileMetadata{}, fmt.Errorf("%w: %v", ErrCorruptGraphData, err)
	}
	if !indexConfigEncodable(index) {
		return FileMetadata{}, ErrGraphLimitExceeded
	}
	if err := validateHNSWIndexVectors(index); err != nil {
		return FileMetadata{}, err
	}
	metadata, err := vhng.Encode(writer, graphToFormat(index, vectors))
	if err != nil {
		return FileMetadata{}, mapFormatError(err)
	}
	return fileMetadata(metadata), nil
}

// OpenGraphFile validates a graph file and its vector-file binding, then opens
// an immutable HNSW index.
func OpenGraphFile(source io.Reader, vectors vectorstore.PreparedVectorStore, vectorFile VectorFileReference, limits GraphLimits) (*HNSWIndex, FileMetadata, error) {
	return OpenGraphFileContext(context.Background(), source, vectors, vectorFile, limits)
}

// OpenGraphFileContext is OpenGraphFile with cancellation for reads,
// decoding, validation, and vector access.
func OpenGraphFileContext(ctx context.Context, source io.Reader, vectors vectorstore.PreparedVectorStore, vectorFile VectorFileReference, limits GraphLimits) (*HNSWIndex, FileMetadata, error) {
	if ctx == nil {
		return nil, FileMetadata{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, FileMetadata{}, err
	}
	if source == nil || vectors == nil || isNilPreparedVectorStore(vectors) {
		return nil, FileMetadata{}, ErrGraphVectorStore
	}
	if !validVectorFileReference(vectorFile) {
		return nil, FileMetadata{}, ErrVectorFileRefMismatch
	}
	limits = normalizeGraphLimits(limits)
	if err := validateGraphLimits(limits); err != nil {
		return nil, FileMetadata{}, err
	}
	decoded, metadata, err := vhng.DecodeContext(ctx, source, formatLimits(limits))
	if err != nil {
		return nil, FileMetadata{}, mapFormatError(err)
	}
	if decoded.Vectors.Size != vectorFile.Size || decoded.Vectors.SHA256 != vectorFile.SHA256 {
		return nil, FileMetadata{}, ErrVectorFileRefMismatch
	}
	index, err := indexFromFormat(ctx, decoded, vectors, limits)
	if err != nil {
		return nil, FileMetadata{}, err
	}
	return index, fileMetadata(metadata), nil
}
