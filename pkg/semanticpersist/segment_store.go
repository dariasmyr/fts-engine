package semanticpersist

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dariasmyr/fts-engine/v2/internal/format"
	"github.com/dariasmyr/fts-engine/v2/internal/memorystore"
	"github.com/dariasmyr/fts-engine/v2/internal/persistfs"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/hnswformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

const segmentObjectIdentityDomain = "semantic-segment-object-v1"

func segmentObjectID(vectors, graph generationformat.FileRef) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(segmentObjectIdentityDomain))

	var size [8]byte
	for _, ref := range []generationformat.FileRef{vectors, graph} {
		binary.LittleEndian.PutUint64(size[:], ref.Size)
		_, _ = hash.Write(size[:])
		_, _ = hash.Write(ref.SHA256[:])
	}

	return "seg-" + hex.EncodeToString(hash.Sum(nil))
}

func validObjectID(id string) bool {
	const prefix = "seg-"
	const hashHexLength = sha256.Size * 2

	if len(id) != len(prefix)+hashHexLength || !strings.HasPrefix(id, prefix) {
		return false
	}
	if filepath.IsAbs(id) || filepath.VolumeName(id) != "" || strings.ContainsAny(id, `/\\`) {
		return false
	}

	decoded, err := hex.DecodeString(strings.TrimPrefix(id, prefix))
	return err == nil && len(decoded) == sha256.Size && id == strings.ToLower(id)
}

type segmentStore struct {
	layout      layout
	limits      Config
	checkpoints saveCheckpoints
}

func newSegmentStore(l layout, cfg settings) segmentStore {
	return segmentStore{layout: l, limits: cfg.limits, checkpoints: cfg.checkpoints}
}

func (s segmentStore) put(ctx context.Context, data semantic.SegmentState, maxK int) (generationformat.SegmentRef, error) {
	segmentTemp, err := os.MkdirTemp(s.layout.segments, ".tmp-seg-")
	if err != nil {
		return generationformat.SegmentRef{}, fmt.Errorf("semanticpersist: create segment temp: %w", err)
	}
	owned := true
	defer func() {
		if owned {
			_ = os.RemoveAll(segmentTemp)
		}
	}()

	if err := s.checkpoints.before(ctx, checkpointWriteVectors); err != nil {
		return generationformat.SegmentRef{}, err
	}
	vectorsRef, err := s.writeVectors(ctx, filepath.Join(segmentTemp, vectorsFileName), data.Vectors, maxK)
	if err != nil {
		return generationformat.SegmentRef{}, err
	}
	if err := s.checkpoints.after(checkpointWriteVectors); err != nil {
		return generationformat.SegmentRef{}, err
	}

	if err := s.checkpoints.before(ctx, checkpointWriteGraph); err != nil {
		return generationformat.SegmentRef{}, err
	}
	if data.Index == nil {
		return generationformat.SegmentRef{}, ErrCorrupt
	}
	graphRef, err := s.writeGraph(ctx, filepath.Join(segmentTemp, graphFileName), data.Index, vectorsRef)
	if err != nil {
		return generationformat.SegmentRef{}, err
	}
	if err := s.checkpoints.after(checkpointWriteGraph); err != nil {
		return generationformat.SegmentRef{}, err
	}

	if err := s.checkpoints.before(ctx, checkpointSyncSegment); err != nil {
		return generationformat.SegmentRef{}, err
	}
	if err := persistfs.SyncDirectory(segmentTemp); err != nil {
		return generationformat.SegmentRef{}, err
	}
	if err := s.checkpoints.after(checkpointSyncSegment); err != nil {
		return generationformat.SegmentRef{}, err
	}

	ref := generationformat.SegmentRef{ObjectID: segmentObjectID(vectorsRef, graphRef), Vectors: vectorsRef, Graph: graphRef}
	storedPath := s.layout.segment(ref.ObjectID)
	if err := s.layout.ensureContained(storedPath); err != nil {
		return generationformat.SegmentRef{}, err
	}
	if err := s.checkpoints.before(ctx, checkpointRenameSegment); err != nil {
		return generationformat.SegmentRef{}, err
	}
	if _, err := os.Lstat(storedPath); err == nil {
		if err := s.verify(ref); err != nil {
			return generationformat.SegmentRef{}, err
		}
		if err := persistfs.SyncDirectory(s.layout.segments); err != nil {
			return generationformat.SegmentRef{}, err
		}
		if err := os.RemoveAll(segmentTemp); err != nil {
			return generationformat.SegmentRef{}, err
		}
		owned = false
	} else if !errors.Is(err, os.ErrNotExist) {
		return generationformat.SegmentRef{}, err
	} else {
		if err := os.Rename(segmentTemp, storedPath); err != nil {
			return generationformat.SegmentRef{}, fmt.Errorf("semanticpersist: install segment: %w", err)
		}
		owned = false
		if err := persistfs.SyncDirectory(s.layout.segments); err != nil {
			return generationformat.SegmentRef{}, err
		}
	}
	if err := s.checkpoints.after(checkpointRenameSegment); err != nil {
		return generationformat.SegmentRef{}, err
	}
	return ref, nil
}

func (s segmentStore) writeVectors(ctx context.Context, path string, source vector.PreparedVectorStore, maxK int) (generationformat.FileRef, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return generationformat.FileRef{}, err
	}
	metadata, writeErr := hnswformat.WriteVectorFile(ctx, file, source, maxK, hnswFormatConfig(s.limits))
	if writeErr == nil {
		writeErr = persistfs.SyncFile(file)
	}
	closeErr := file.Close()
	if writeErr != nil {
		return generationformat.FileRef{}, mapFormatError(writeErr)
	}
	if closeErr != nil {
		return generationformat.FileRef{}, closeErr
	}
	if metadata.Size > s.limits.MaxFileBytes || metadata.Size > s.limits.MaxVectorBytes+128 {
		return generationformat.FileRef{}, ErrLimitExceeded
	}
	return metadata.FileRef, nil
}

func (s segmentStore) writeGraph(ctx context.Context, path string, index *hnsw.Index, vectors generationformat.FileRef) (generationformat.FileRef, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return generationformat.FileRef{}, err
	}
	metadata, writeErr := hnswformat.WriteGraphFile(ctx, file, index, vectors)
	if writeErr == nil {
		writeErr = persistfs.SyncFile(file)
	}
	closeErr := file.Close()
	if writeErr != nil {
		return generationformat.FileRef{}, mapFormatError(writeErr)
	}
	if closeErr != nil {
		return generationformat.FileRef{}, closeErr
	}
	ref := generationformat.FileRef{Size: metadata.Size, SHA256: metadata.SHA256}
	if ref.Size > min(s.limits.MaxFileBytes, s.limits.MaxGraphBytes) {
		return generationformat.FileRef{}, ErrLimitExceeded
	}
	return ref, nil
}

func (s segmentStore) verify(ref generationformat.SegmentRef) error {
	if !validObjectID(ref.ObjectID) || ref.ObjectID != segmentObjectID(ref.Vectors, ref.Graph) {
		return ErrCorrupt
	}
	path := s.layout.segment(ref.ObjectID)
	if err := s.layout.ensureContained(path); err != nil {
		return err
	}
	if err := validateDirectory(path); err != nil {
		return corruptMissingReference(err)
	}
	if err := verifyReferencedFile(filepath.Join(path, vectorsFileName), ref.Vectors, min(s.limits.MaxFileBytes, s.limits.MaxVectorBytes+128)); err != nil {
		return err
	}
	if err := verifyReferencedFile(filepath.Join(path, graphFileName), ref.Graph, min(s.limits.MaxFileBytes, s.limits.MaxGraphBytes)); err != nil {
		return err
	}
	return persistfs.SyncDirectory(path)
}

func (s segmentStore) open(ctx context.Context, ref generationformat.SegmentRef, config semantic.SemanticConfig, state generationformat.SegmentState) (semantic.SegmentState, error) {
	if !validObjectID(ref.ObjectID) || ref.ObjectID != segmentObjectID(ref.Vectors, ref.Graph) {
		return semantic.SegmentState{}, ErrCorrupt
	}
	path := s.layout.segment(ref.ObjectID)
	if err := s.layout.ensureContained(path); err != nil {
		return semantic.SegmentState{}, err
	}
	if err := validateDirectory(path); err != nil {
		return semantic.SegmentState{}, corruptMissingReference(err)
	}
	vectorsData, err := readReferencedFileWithoutHash(filepath.Join(path, vectorsFileName), ref.Vectors, min(s.limits.MaxFileBytes, s.limits.MaxVectorBytes+128))
	if err != nil {
		return semantic.SegmentState{}, err
	}
	decoded, metadata, err := hnswformat.DecodeVectorFile(ctx, vectorsData, hnswFormatConfig(s.limits))
	if err != nil {
		return semantic.SegmentState{}, mapFormatError(err)
	}
	if metadata.Size != ref.Vectors.Size || metadata.SHA256 != ref.Vectors.SHA256 {
		return semantic.SegmentState{}, ErrCorrupt
	}
	vectors, err := memorystore.NewPrepared(decoded.Space, decoded.Values)
	if err != nil {
		return semantic.SegmentState{}, ErrCorrupt
	}
	graphData, err := readReferencedFileWithoutHash(filepath.Join(path, graphFileName), ref.Graph, min(s.limits.MaxFileBytes, s.limits.MaxGraphBytes))
	if err != nil {
		return semantic.SegmentState{}, err
	}
	index, graphMetadata, err := hnswformat.OpenGraphFile(
		ctx,
		graphData,
		vectors,
		ref.Vectors,
		hnsw.Config{
			Build: hnsw.BuildConfig{
				MaxNeighbors:   config.Index.HNSW.MaxNeighbors,
				EfConstruction: config.Index.HNSW.EfConstruction,
				Seed:           config.Index.HNSW.Seed,
			},
			Search: hnsw.SearchConfig{
				EfSearch:   config.Index.HNSW.DefaultEfSearch,
				VisitLimit: config.Index.HNSW.DefaultVisitLimit,
			},
		},
		hnswFormatConfig(s.limits),
	)
	if err != nil {
		return semantic.SegmentState{}, mapFormatError(err)
	}
	if graphMetadata.Size != ref.Graph.Size || graphMetadata.SHA256 != ref.Graph.SHA256 {
		return semantic.SegmentState{}, ErrCorrupt
	}
	segment := semantic.SegmentState{
		ID:      state.ID,
		Rows:    state.Rows,
		Vectors: vectors,
		Index:   index,
	}
	if err := validateSegmentData(segment, config, s.limits); err != nil {
		return semantic.SegmentState{}, err
	}
	return segment, nil
}

func validateSegmentData(data semantic.SegmentState, config semantic.SemanticConfig, limits Config) error {
	semanticLimits := config.Index.Limits
	if data.Vectors == nil || data.Index == nil {
		return ErrLimitExceeded
	}
	dimensions := data.Index.Dimensions()
	length := data.Index.Len()
	topology := hnsw.Snapshot(data.Index)
	if dimensions <= 0 || dimensions > limits.MaxDimensions || length < 0 || length > limits.MaxVectors ||
		semanticLimits.MaxLiveVectors <= 0 || semanticLimits.MaxStaleVectors < 0 ||
		semanticLimits.MaxLiveVectors > limits.MaxVectors-semanticLimits.MaxStaleVectors ||
		semanticLimits.MaxSegments <= 0 || semanticLimits.MaxSegments > limits.MaxVectors ||
		config.Index.Search.MaxResults <= 0 || config.Index.Search.MaxResults > limits.MaxK ||
		config.Index.Search.MaxCandidateChunks < config.Index.Search.MaxResults || config.Index.Search.MaxCandidateChunks > limits.MaxK ||
		config.Index.Search.MaxChunksPerResult <= 0 || config.Index.Search.MaxChunksPerResult > limits.MaxChunksPerDocument {
		return ErrLimitExceeded
	}
	components, ok := format.CheckedMultiplyUint64(uint64(length), uint64(dimensions))
	if !ok {
		return ErrLimitExceeded
	}
	vectorBytes, ok := format.CheckedMultiplyUint64(components, 4)
	if !ok || vectorBytes > limits.MaxVectorBytes || limits.MaxFileBytes < 44 || vectorBytes > limits.MaxFileBytes-44 {
		return ErrLimitExceeded
	}
	directedLinks, ok := format.CheckedAddUint64(uint64(len(topology.Level0Links)), uint64(len(topology.UpperLinks)))
	if !ok || config.Index.HNSW.MaxEfSearch > limits.MaxEfSearch || config.Index.HNSW.MaxVisitLimit > limits.MaxVisitLimit ||
		topology.MaxNeighbors != config.Index.HNSW.MaxNeighbors || topology.LevelZeroMaxNeighbors != config.Index.HNSW.MaxNeighbors*2 ||
		topology.EfConstruction != config.Index.HNSW.EfConstruction || topology.Seed != config.Index.HNSW.Seed ||
		directedLinks > limits.MaxGraphLinks {
		return ErrLimitExceeded
	}
	graphBytes, err := hnswformat.GraphFileSize(topology, generationformat.FileRef{Size: 1, SHA256: [32]byte{1}})
	if err != nil || graphBytes > min(limits.MaxFileBytes, limits.MaxGraphBytes) {
		return ErrLimitExceeded
	}
	return nil
}
