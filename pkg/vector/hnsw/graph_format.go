package hnsw

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	vhng "github.com/dariasmyr/fts-engine/pkg/vector/hnsw/internal/format"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

const (
	graphFormatMagic      = vhng.Magic
	graphFormatVersion    = vhng.Version
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

// WriteGraph writes exact packed topology and configuration. Vector values are
// not included; vectors identifies their authoritative separate file.
func WriteGraph(ctx context.Context, writer io.Writer, index *Index, vectors VectorFileReference) (FileMetadata, error) {
	if ctx == nil {
		return FileMetadata{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return FileMetadata{}, err
	}
	if writer == nil || index == nil || !validVectorFileReference(vectors) {
		return FileMetadata{}, ErrCorruptGraphData
	}
	if _, err := validatePackedTopologyContext(ctx, index); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return FileMetadata{}, err
		}
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
	if err := validateIndexVectorsContext(ctx, index); err != nil {
		return FileMetadata{}, err
	}
	metadata, err := vhng.Encode(contextWriter{ctx: ctx, writer: writer}, graphToFormat(index, vectors))
	if err != nil {
		return FileMetadata{}, mapFormatError(err)
	}
	return fileMetadata(metadata), nil
}

// OpenGraph validates a graph stream and its vector-file binding, then opens an
// immutable HNSW index.
func OpenGraph(ctx context.Context, source io.Reader, vectors vectorstore.PreparedVectorStore, vectorFile VectorFileReference, limits GraphLimits) (*Index, FileMetadata, error) {
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

type contextWriter struct {
	ctx    context.Context
	writer io.Writer
}

func (w contextWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.writer.Write(p)
}
