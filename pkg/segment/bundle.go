package segment

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

const bundleVersion uint16 = 1

type LoadedBundle struct {
	Segment         *Reader
	CollectionStats *fts.CollectionStatsSnapshot
	Registry        []fts.DocID
	Tombstones      []uint64
}

type bundleEnvelope struct {
	Version         uint16
	SegmentPayload  []byte
	CollectionStats *fts.CollectionStatsSnapshot
	Registry        []fts.DocID
	Tombstones      []uint64
}

func SaveBundle(w io.Writer, source Source, stats *fts.CollectionStatsSnapshot, registry []fts.DocID, tombstones []uint64) error {
	if w == nil {
		return fmt.Errorf("segment: save bundle: nil writer")
	}

	segmentBytes, err := BuildFromSource(source)
	if err != nil {
		return fmt.Errorf("segment: save bundle: build segment: %w", err)
	}

	envelope := bundleEnvelope{
		Version:         bundleVersion,
		SegmentPayload:  segmentBytes,
		CollectionStats: stats,
		Registry:        append([]fts.DocID(nil), registry...),
		Tombstones:      append([]uint64(nil), tombstones...),
	}
	if err := gob.NewEncoder(w).Encode(envelope); err != nil {
		return fmt.Errorf("segment: save bundle: encode envelope: %w", err)
	}
	return nil
}

func LoadBundle(r io.Reader) (*LoadedBundle, error) {
	if r == nil {
		return nil, fmt.Errorf("segment: load bundle: nil reader")
	}

	var envelope bundleEnvelope
	if err := gob.NewDecoder(r).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("segment: load bundle: decode envelope: %w", err)
	}
	if envelope.Version != bundleVersion {
		return nil, fmt.Errorf("segment: load bundle: unsupported version %d", envelope.Version)
	}

	reader, err := Open(envelope.SegmentPayload)
	if err != nil {
		return nil, fmt.Errorf("segment: load bundle: open segment payload: %w", err)
	}

	return &LoadedBundle{
		Segment:         reader,
		CollectionStats: envelope.CollectionStats,
		Registry:        append([]fts.DocID(nil), envelope.Registry...),
		Tombstones:      append([]uint64(nil), envelope.Tombstones...),
	}, nil
}

func RestoreService(bundle *LoadedBundle, keyGen fts.KeyGenerator, opts ...fts.Option) (*fts.Service, error) {
	if bundle == nil {
		return nil, fmt.Errorf("segment: restore service: nil bundle")
	}
	if bundle.Segment == nil {
		return nil, fmt.Errorf("segment: restore service: nil segment reader")
	}

	builtOpts := append([]fts.Option(nil), opts...)
	if len(bundle.Registry) > 0 {
		builtOpts = append(builtOpts, fts.WithDocRegistrySnapshot(bundle.Registry))
	}
	if len(bundle.Tombstones) > 0 {
		builtOpts = append(builtOpts, fts.WithTombstonesSnapshot(bundle.Tombstones))
	}
	if bundle.CollectionStats != nil {
		builtOpts = append(builtOpts, fts.WithCollectionStatsSnapshot(bundle.CollectionStats))
	}

	return fts.New(bundle.Segment, keyGen, builtOpts...), nil
}

func SaveBundleToBytes(source Source, stats *fts.CollectionStatsSnapshot, registry []fts.DocID, tombstones []uint64) ([]byte, error) {
	var buf bytes.Buffer
	if err := SaveBundle(&buf, source, stats, registry, tombstones); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
