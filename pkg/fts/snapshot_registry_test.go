package fts

import "io"

var testSnapshotRegistry = NewSnapshotRegistry()

func RegisterIndexSnapshotCodec(name string, saver IndexSnapshotSaver, loader IndexSnapshotLoader) error {
	return testSnapshotRegistry.RegisterIndexSnapshotCodec(name, saver, loader)
}

func RegisterFilterSnapshotCodec(name string, saver FilterSnapshotSaver, loader FilterSnapshotLoader) error {
	return testSnapshotRegistry.RegisterFilterSnapshotCodec(name, saver, loader)
}

func SaveIndexSnapshotWithState(w io.Writer, indexName string, index Index, stats *CollectionStatsSnapshot, registry []DocID, tombstones []uint64) error {
	return testSnapshotRegistry.SaveIndexSnapshotWithState(
		w, indexName, index, stats, registry, tombstones,
		map[string]AnalyzerDescriptor{DefaultField: defaultPipeline{}.Descriptor()},
		keyGeneratorDescriptorForTests(),
	)
}

func LoadIndexSnapshot(r io.Reader) (*LoadedIndexSnapshot, error) {
	return testSnapshotRegistry.LoadIndexSnapshot(r)
}

func SaveMultiIndexSnapshotWithState(w io.Writer, fieldCodecs map[string]string, indexes map[string]Index, stats *CollectionStatsSnapshot, registry []DocID, tombstones []uint64) error {
	analyzers := make(map[string]AnalyzerDescriptor, len(indexes))
	for fieldName := range indexes {
		analyzers[fieldName] = defaultPipeline{}.Descriptor()
	}
	return testSnapshotRegistry.SaveMultiIndexSnapshotWithState(
		w, fieldCodecs, indexes, stats, registry, tombstones, analyzers, keyGeneratorDescriptorForTests(),
	)
}

func keyGeneratorDescriptorForTests() *KeyGeneratorDescriptor {
	descriptor := NewKeyGeneratorDescriptor("word-keys", 1)
	return &descriptor
}

func LoadMultiIndexSnapshot(r io.Reader) (*LoadedMultiIndexSnapshot, error) {
	return testSnapshotRegistry.LoadMultiIndexSnapshot(r)
}

func SaveFilterSnapshot(w io.Writer, filterName string, filter Filter) error {
	return testSnapshotRegistry.SaveFilterSnapshot(w, filterName, filter)
}

func LoadFilterSnapshot(r io.Reader) (*LoadedFilterSnapshot, error) {
	return testSnapshotRegistry.LoadFilterSnapshot(r)
}
