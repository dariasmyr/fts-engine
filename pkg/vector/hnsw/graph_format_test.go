package hnsw

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

func testVectorFileReference() VectorFileReference {
	return VectorFileReference{Size: 1234, SHA256: sha256.Sum256([]byte("authoritative-vector-file"))}
}

func marshalGraph(index *Index, vectors VectorFileReference) ([]byte, FileMetadata, error) {
	var buffer bytes.Buffer
	metadata, err := WriteGraph(context.Background(), &buffer, index, vectors)
	return buffer.Bytes(), metadata, err
}

func openGraph(source io.Reader, vectors vectorstore.PreparedVectorStore, reference VectorFileReference, limits GraphLimits) (*Index, FileMetadata, error) {
	return OpenGraph(context.Background(), source, vectors, reference, limits)
}

func TestGraphFormatRoundTripExactTopologyAndSearch(t *testing.T) {
	original := newReaderForTest(t, readerTestSpace(t, 2, vector.MetricL2Squared), readerTestGraph())
	reference := testVectorFileReference()
	first, firstMetadata, err := marshalGraph(original, reference)
	if err != nil {
		t.Fatal(err)
	}
	second, secondMetadata, err := marshalGraph(original, reference)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) || firstMetadata != secondMetadata {
		t.Fatal("graph encoding is not deterministic")
	}
	if firstMetadata.Size != uint64(len(first)) || firstMetadata.CRC32 != crc32.ChecksumIEEE(first[:len(first)-graphFormatFooterSize]) || firstMetadata.SHA256 != sha256.Sum256(first) {
		t.Fatalf("metadata = %+v", firstMetadata)
	}

	opened, openedMetadata, err := openGraph(bytes.NewReader(first), original.vectors, reference, DefaultGraphLimits())
	if err != nil {
		t.Fatal(err)
	}
	if openedMetadata != firstMetadata || !reflect.DeepEqual(opened.Report(), original.Report()) {
		t.Fatalf("opened metadata/configuration differ: metadata=%+v report=%+v", openedMetadata, opened.Report())
	}
	if !slices.Equal(opened.topology.nodeToVector, original.topology.nodeToVector) || !slices.Equal(opened.topology.levels, original.topology.levels) ||
		!slices.Equal(opened.topology.level0Offsets, original.topology.level0Offsets) || !slices.Equal(opened.topology.level0Neighbors, original.topology.level0Neighbors) ||
		!slices.Equal(opened.topology.upperNodeOffsets, original.topology.upperNodeOffsets) || !slices.Equal(opened.topology.upperLinkOffsets, original.topology.upperLinkOffsets) ||
		!slices.Equal(opened.topology.upperNeighbors, original.topology.upperNeighbors) {
		t.Fatal("packed topology changed across graph format round trip")
	}
	for _, query := range [][]float32{{0, 0}, {1.5, 0}, {4, 0}} {
		want, err := original.Search(context.Background(), query, 4, vector.SearchOptions{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := opened.Search(context.Background(), query, 4, vector.SearchOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round-trip search(%v) = %+v, want %+v", query, got, want)
		}
	}
}

func TestGraphFormatEmptySingletonAndCosine(t *testing.T) {
	reference := testVectorFileReference()
	spaces := []struct {
		name       string
		calculator vector.Calculator
		graph      graphData
		query      []float32
		want       []vector.Hit
	}{
		{name: "empty l2", calculator: readerTestSpace(t, 2, vector.MetricL2Squared), graph: graphData{}, query: []float32{0, 0}},
		{name: "singleton cosine", calculator: readerTestSpace(t, 2, vector.MetricCosine), graph: graphData{values: []float32{0.6, 0.8}, nodes: []mutableNode{{links: [][]nodeOrdinal{{}}}}, hasEntry: true}, query: []float32{3, 4}, want: []vector.Hit{{Ordinal: 0}}},
	}
	for _, test := range spaces {
		t.Run(test.name, func(t *testing.T) {
			original := newReaderForTest(t, test.calculator, test.graph)
			data, metadata, err := marshalGraph(original, reference)
			if err != nil {
				t.Fatal(err)
			}
			opened, gotMetadata, err := openGraph(bytes.NewReader(data), original.vectors, reference, DefaultGraphLimits())
			if err != nil {
				t.Fatal(err)
			}
			if gotMetadata != metadata || opened.Len() != original.Len() || opened.Metric() != original.Metric() || opened.vectors.Normalization() != original.vectors.Normalization() {
				t.Fatalf("opened reader = len %d metric %v normalization %v metadata %+v", opened.Len(), opened.Metric(), opened.vectors.Normalization(), gotMetadata)
			}
			result, err := opened.Search(context.Background(), test.query, 1, vector.SearchOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(result.Hits, test.want) {
				t.Fatalf("hits = %+v, want %+v", result.Hits, test.want)
			}
		})
	}
}

type graphPreparedSource struct {
	values        [][]float32
	dimensions    int
	metric        vector.Metric
	normalization vector.Normalization
	partial       bool
	reads         int
	cancel        context.CancelFunc
}

func (s *graphPreparedSource) Len() int                            { return len(s.values) }
func (s *graphPreparedSource) Dimensions() int                     { return s.dimensions }
func (s *graphPreparedSource) Metric() vector.Metric               { return s.metric }
func (s *graphPreparedSource) Normalization() vector.Normalization { return s.normalization }
func (s *graphPreparedSource) ReadVectorInto(_ context.Context, ordinal vector.Ordinal, dst []float32) error {
	s.reads++
	if s.cancel != nil {
		s.cancel()
	}
	if s.partial {
		dst[0] = s.values[ordinal][0]
		return nil
	}
	copy(dst, s.values[ordinal])
	return nil
}

func TestOpenGraphValidatesPreparedVectorStore(t *testing.T) {
	original := newReaderForTest(t, readerTestSpace(t, 2, vector.MetricL2Squared), readerTestGraph())
	reference := testVectorFileReference()
	data, _, err := marshalGraph(original, reference)
	if err != nil {
		t.Fatal(err)
	}
	values := [][]float32{{0, 0}, {1, 0}, {2, 0}, {3, 0}}
	source := &graphPreparedSource{values: values, dimensions: 2, metric: vector.MetricL2Squared, normalization: vector.NormalizationNone}
	opened, _, err := openGraph(bytes.NewReader(data), source, reference, DefaultGraphLimits())
	if err != nil {
		t.Fatal(err)
	}
	got, _ := readPreparedVector(opened.vectors, 0)
	if !slices.Equal(got, []float32{0, 0}) || source.reads < len(values) {
		t.Fatalf("opened vectors/source validation = value:%v reads:%d", got, source.reads)
	}

	for _, mismatch := range []*graphPreparedSource{
		{values: values[:3], dimensions: 2, metric: vector.MetricL2Squared, normalization: vector.NormalizationNone},
		{values: values, dimensions: 1, metric: vector.MetricL2Squared, normalization: vector.NormalizationNone},
		{values: values, dimensions: 2, metric: vector.MetricCosine, normalization: vector.NormalizationUnitLength},
	} {
		if _, _, err := openGraph(bytes.NewReader(data), mismatch, reference, DefaultGraphLimits()); !errors.Is(err, ErrGraphVectorStore) || mismatch.reads != 0 {
			t.Fatalf("source mismatch error/reads = %v/%d", err, mismatch.reads)
		}
	}
	partial := &graphPreparedSource{values: values, dimensions: 2, metric: vector.MetricL2Squared, partial: true}
	if _, _, err := openGraph(bytes.NewReader(data), partial, reference, DefaultGraphLimits()); !errors.Is(err, ErrGraphVectorStore) {
		t.Fatalf("partial source row error = %v", err)
	}
}

func TestGraphFormatRejectsReferenceSizeAndIntegrityFailures(t *testing.T) {
	original := newReaderForTest(t, readerTestSpace(t, 2, vector.MetricL2Squared), readerTestGraph())
	reference := testVectorFileReference()
	data, _, err := marshalGraph(original, reference)
	if err != nil {
		t.Fatal(err)
	}
	wrong := reference
	wrong.Size++
	if _, _, err := openGraph(bytes.NewReader(data), original.vectors, wrong, DefaultGraphLimits()); !errors.Is(err, ErrVectorFileRefMismatch) {
		t.Fatalf("reference mismatch error = %v", err)
	}
	if _, _, err := marshalGraph(original, VectorFileReference{}); !errors.Is(err, ErrCorruptGraphData) {
		t.Fatalf("empty reference marshal error = %v", err)
	}
	for _, size := range []int{0, 3, graphFormatHeaderSize - 1, len(data) - 1} {
		if _, _, err := openGraph(bytes.NewReader(data[:size]), original.vectors, reference, DefaultGraphLimits()); err == nil {
			t.Fatalf("truncated graph size %d was accepted", size)
		}
	}
	trailing := append(append([]byte(nil), data...), 0)
	if _, _, err := openGraph(bytes.NewReader(trailing), original.vectors, reference, DefaultGraphLimits()); !errors.Is(err, ErrCorruptGraphData) {
		t.Fatalf("trailing graph error = %v", err)
	}
	corrupt := append([]byte(nil), data...)
	corrupt[graphFormatHeaderSize] ^= 1
	if _, _, err := openGraph(bytes.NewReader(corrupt), original.vectors, reference, DefaultGraphLimits()); !errors.Is(err, ErrCorruptGraphData) {
		t.Fatalf("CRC corruption error = %v", err)
	}
	unsupported := append([]byte(nil), data...)
	binary.LittleEndian.PutUint16(unsupported[4:6], GraphFormatVersion+1)
	if _, _, err := openGraph(bytes.NewReader(unsupported), original.vectors, reference, DefaultGraphLimits()); !errors.Is(err, ErrUnsupportedGraphVersion) {
		t.Fatalf("graph format version error = %v", err)
	}
}

func TestGraphFormatRejectsMalformedPackedTopologyAndCanonicalBytes(t *testing.T) {
	original := newReaderForTest(t, readerTestSpace(t, 2, vector.MetricL2Squared), readerTestGraph())
	reference := testVectorFileReference()
	valid, _, err := marshalGraph(original, reference)
	if err != nil {
		t.Fatal(err)
	}
	sections := graphFormatTestSections(valid)
	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{"reserved header", func(data []byte) { data[144] = 1 }},
		{"entry flag", func(data []byte) { data[14] = 2 }},
		{"entry out of range", func(data []byte) { binary.LittleEndian.PutUint32(data[20:24], 99) }},
		{"mapping duplicate", func(data []byte) {
			copy(data[sections.mapping+4:sections.mapping+8], data[sections.mapping:sections.mapping+4])
		}},
		{"level too high", func(data []byte) { data[sections.levels] = MaxLevel + 1 }},
		{"level-0 offsets", func(data []byte) {
			binary.LittleEndian.PutUint32(data[sections.level0Offsets:sections.level0Offsets+4], 1)
		}},
		{"level-0 degree", func(data []byte) {
			for i := 1; i < 5; i++ {
				binary.LittleEndian.PutUint32(data[sections.level0Offsets+i*4:sections.level0Offsets+(i+1)*4], 5)
			}
		}},
		{"level-0 self link", func(data []byte) { binary.LittleEndian.PutUint32(data[sections.level0Links:sections.level0Links+4], 0) }},
		{"duplicate level-0 link", func(data []byte) {
			binary.LittleEndian.PutUint32(data[sections.level0Links+4:sections.level0Links+8], 1)
		}},
		{"upper-node offsets", func(data []byte) {
			binary.LittleEndian.PutUint32(data[sections.upperNodeOffsets:sections.upperNodeOffsets+4], 1)
		}},
		{"impossible upper placements", func(data []byte) {
			nodes := binary.LittleEndian.Uint32(data[16:20])
			binary.LittleEndian.PutUint32(data[116:120], nodes*MaxLevel+1)
		}},
		{"upper-link offsets", func(data []byte) {
			binary.LittleEndian.PutUint32(data[sections.upperLinkOffsets:sections.upperLinkOffsets+4], 1)
		}},
		{"upper link to absent level", func(data []byte) { binary.LittleEndian.PutUint32(data[sections.upperLinks:sections.upperLinks+4], 1) }},
		{"maximum level", func(data []byte) { data[124]-- }},
		{"missing build provenance", func(data []byte) { binary.LittleEndian.PutUint32(data[44:48], 0) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := append([]byte(nil), valid...)
			test.mutate(data)
			rewriteGraphChecksum(data)
			if _, _, err := openGraph(bytes.NewReader(data), original.vectors, reference, DefaultGraphLimits()); !errors.Is(err, ErrCorruptGraphData) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	futureBuild := append([]byte(nil), valid...)
	binary.LittleEndian.PutUint32(futureBuild[44:48], BuildVersion+1)
	rewriteGraphChecksum(futureBuild)
	opened, _, err := openGraph(bytes.NewReader(futureBuild), original.vectors, reference, DefaultGraphLimits())
	if err != nil || opened.Report().Build.BuildVersion != BuildVersion+1 {
		t.Fatalf("future build provenance = (%+v, %v)", opened, err)
	}

	singleton := newReaderForTest(t, readerTestSpace(t, 2, vector.MetricL2Squared), graphData{values: []float32{1, 2}, nodes: []mutableNode{{links: [][]nodeOrdinal{{}}}}, hasEntry: true})
	padded, _, err := marshalGraph(singleton, reference)
	if err != nil {
		t.Fatal(err)
	}
	padded[graphFormatHeaderSize+4+1] = 1
	rewriteGraphChecksum(padded)
	if _, _, err := openGraph(bytes.NewReader(padded), singleton.vectors, reference, DefaultGraphLimits()); !errors.Is(err, ErrCorruptGraphData) {
		t.Fatalf("non-canonical level padding error = %v", err)
	}
}

func TestOpenGraphContextCancellation(t *testing.T) {
	original := newReaderForTest(t, readerTestSpace(t, 2, vector.MetricL2Squared), readerTestGraph())
	reference := testVectorFileReference()
	data, _, err := marshalGraph(original, reference)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := OpenGraph(ctx, bytes.NewReader(data), original.vectors, reference, DefaultGraphLimits()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled open error = %v", err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	source := &graphPreparedSource{
		values: [][]float32{{0, 0}, {1, 0}, {2, 0}, {3, 0}}, dimensions: 2,
		metric: vector.MetricL2Squared, cancel: cancel,
	}
	if _, _, err := OpenGraph(ctx, bytes.NewReader(data), source, reference, DefaultGraphLimits()); !errors.Is(err, context.Canceled) {
		t.Fatalf("source-cancelled open error = %v", err)
	}
	if _, _, err := OpenGraph(nil, bytes.NewReader(data), original.vectors, reference, DefaultGraphLimits()); !errors.Is(err, vector.ErrNilContext) {
		t.Fatalf("nil context open error = %v", err)
	}
}

func TestWriteGraphContextCancellation(t *testing.T) {
	index := newReaderForTest(t, readerTestSpace(t, 2, vector.MetricL2Squared), readerTestGraph())
	topologyCtx := newCancelOnErrContext(2)
	if _, err := WriteGraph(topologyCtx, io.Discard, index, testVectorFileReference()); !errors.Is(err, context.Canceled) {
		t.Fatalf("topology-cancelled write error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	writer := cancelAfterWrite{cancel: cancel}
	if _, err := WriteGraph(ctx, &writer, index, testVectorFileReference()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled write error = %v", err)
	}
	if writer.writes != 1 {
		t.Fatalf("writes after cancellation = %d, want 1", writer.writes)
	}
	ctx, cancel = context.WithCancel(context.Background())
	source := &graphPreparedSource{
		values: [][]float32{{0, 0}, {1, 0}, {2, 0}, {3, 0}}, dimensions: 2,
		metric: vector.MetricL2Squared, cancel: cancel,
	}
	index, err := newIndexFromGraph(readerTestSpace(t, 2, vector.MetricL2Squared), readerTestSearchConfig(), readerTestBuildInfo(), readerTestGraph(), source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteGraph(ctx, io.Discard, index, testVectorFileReference()); !errors.Is(err, context.Canceled) {
		t.Fatalf("source-cancelled write error = %v", err)
	}
	if _, err := WriteGraph(nil, io.Discard, index, testVectorFileReference()); !errors.Is(err, vector.ErrNilContext) {
		t.Fatalf("nil context write error = %v", err)
	}
}

func TestMalformedTopologyIsRejectedBeforeSourceRowsAreRead(t *testing.T) {
	original := newReaderForTest(t, readerTestSpace(t, 2, vector.MetricL2Squared), readerTestGraph())
	reference := testVectorFileReference()
	data, _, err := marshalGraph(original, reference)
	if err != nil {
		t.Fatal(err)
	}
	sections := graphFormatTestSections(data)
	binary.LittleEndian.PutUint32(data[sections.level0Offsets:sections.level0Offsets+4], 1)
	rewriteGraphChecksum(data)
	source := &graphPreparedSource{values: [][]float32{{0, 0}, {1, 0}, {2, 0}, {3, 0}}, dimensions: 2, metric: vector.MetricL2Squared}
	if _, _, err := openGraph(bytes.NewReader(data), source, reference, DefaultGraphLimits()); !errors.Is(err, ErrCorruptGraphData) || source.reads != 0 {
		t.Fatalf("malformed topology error/reads = %v/%d", err, source.reads)
	}
}

func TestOpenGraphEnforcesConfiguredLimits(t *testing.T) {
	original := newReaderForTest(t, readerTestSpace(t, 2, vector.MetricL2Squared), readerTestGraph())
	reference := testVectorFileReference()
	data, _, err := marshalGraph(original, reference)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		change func(*GraphLimits)
	}{
		{"dimensions", func(l *GraphLimits) { l.MaxDimensions = 1 }},
		{"vectors", func(l *GraphLimits) { l.MaxVectors = 3 }},
		{"vector bytes", func(l *GraphLimits) { l.MaxVectorBytes = 31 }},
		{"graph bytes", func(l *GraphLimits) { l.MaxGraphBytes = uint64(len(data) - 1) }},
		{"links", func(l *GraphLimits) { l.MaxLinks = 6 }},
		{"level", func(l *GraphLimits) { l.MaxLevel = 1 }},
		{"ef search", func(l *GraphLimits) { l.MaxEfSearch = 31 }},
		{"visit limit", func(l *GraphLimits) { l.MaxVisitLimit = 127 }},
		{"k", func(l *GraphLimits) { l.MaxK = 15 }},
		{"construction ef", func(l *GraphLimits) { l.MaxEfConstruction = 7 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limits := DefaultGraphLimits()
			test.change(&limits)
			if _, _, err := openGraph(bytes.NewReader(data), original.vectors, reference, limits); !errors.Is(err, ErrGraphLimitExceeded) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestOpenedReaderConcurrentSearch(t *testing.T) {
	original := newReaderForTest(t, readerTestSpace(t, 2, vector.MetricL2Squared), readerTestGraph())
	reference := testVectorFileReference()
	data, _, err := marshalGraph(original, reference)
	if err != nil {
		t.Fatal(err)
	}
	opened, _, err := openGraph(bytes.NewReader(data), original.vectors, reference, DefaultGraphLimits())
	if err != nil {
		t.Fatal(err)
	}
	want, err := original.Search(context.Background(), []float32{1.25, 0}, 4, vector.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	errorsFound := make(chan error, 16)
	for range 16 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 100 {
				got, err := opened.Search(context.Background(), []float32{1.25, 0}, 4, vector.SearchOptions{})
				if err != nil {
					errorsFound <- err
					return
				}
				if !reflect.DeepEqual(got, want) {
					errorsFound <- errors.New("concurrent search result changed")
					return
				}
			}
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
}

func TestWriteGraphHandlesShortWrites(t *testing.T) {
	reader := newReaderForTest(t, readerTestSpace(t, 1, vector.MetricL2Squared), graphData{})
	if _, err := WriteGraph(context.Background(), zeroWriter{}, reader, testVectorFileReference()); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write error = %v", err)
	}
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

type cancelAfterWrite struct {
	cancel context.CancelFunc
	writes int
}

func (w *cancelAfterWrite) Write(p []byte) (int, error) {
	w.writes++
	w.cancel()
	return len(p), nil
}

type graphFormatSections struct {
	mapping, levels, level0Offsets, level0Links    int
	upperNodeOffsets, upperLinkOffsets, upperLinks int
}

func graphFormatTestSections(data []byte) graphFormatSections {
	nodes := int(binary.LittleEndian.Uint32(data[16:20]))
	level0Links := int(binary.LittleEndian.Uint32(data[112:116]))
	upperPlacements := int(binary.LittleEndian.Uint32(data[116:120]))
	sections := graphFormatSections{mapping: graphFormatHeaderSize}
	sections.levels = sections.mapping + nodes*4
	sections.level0Offsets = sections.levels + int(aligned4(uint64(nodes)))
	sections.level0Links = sections.level0Offsets + (nodes+1)*4
	sections.upperNodeOffsets = sections.level0Links + level0Links*4
	sections.upperLinkOffsets = sections.upperNodeOffsets + (nodes+1)*4
	sections.upperLinks = sections.upperLinkOffsets + (upperPlacements+1)*4
	return sections
}

func rewriteGraphChecksum(data []byte) {
	body := data[:len(data)-graphFormatFooterSize]
	binary.LittleEndian.PutUint32(data[len(data)-graphFormatFooterSize:], crc32.ChecksumIEEE(body))
}

func FuzzOpenGraph(f *testing.F) {
	reference := testVectorFileReference()
	space, err := vector.NewCalculator(2, vector.MetricL2Squared)
	if err != nil {
		f.Fatal(err)
	}
	empty, err := newIndexFromGraph(space, readerTestSearchConfig(), readerTestBuildInfo(), graphData{})
	if err != nil {
		f.Fatal(err)
	}
	singleton, err := newIndexFromGraph(space, readerTestSearchConfig(), readerTestBuildInfo(), graphData{values: []float32{1, 2}, nodes: []mutableNode{{links: [][]nodeOrdinal{{}}}}, hasEntry: true})
	if err != nil {
		f.Fatal(err)
	}
	emptyData, _, err := marshalGraph(empty, reference)
	if err != nil {
		f.Fatal(err)
	}
	singletonData, _, err := marshalGraph(singleton, reference)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(emptyData, uint8(0))
	f.Add(singletonData, uint8(1))
	f.Add([]byte(graphFormatMagic), uint8(0))
	f.Fuzz(func(t *testing.T, data []byte, sourceKind uint8) {
		limits := GraphLimits{
			MaxDimensions: 8, MaxVectors: 16, MaxVectorBytes: 512, MaxGraphBytes: 64 << 10,
			MaxLinks: 256, MaxLevel: MaxLevel, MaxEfSearch: 64, MaxVisitLimit: 256,
			MaxK: 32, MaxNeighbors: 8, MaxEfConstruction: 64,
		}
		source := vectorstore.PreparedVectorStore(empty.vectors)
		if sourceKind&1 != 0 {
			source = singleton.vectors
		}
		_, _, _ = openGraph(bytes.NewReader(data), source, reference, limits)
	})
}

func TestGraphFormatSourceRejectsNonCanonicalNegativeZero(t *testing.T) {
	original := newReaderForTest(t, readerTestSpace(t, 1, vector.MetricL2Squared), graphData{values: []float32{1}, nodes: []mutableNode{{links: [][]nodeOrdinal{{}}}}, hasEntry: true})
	data, _, err := marshalGraph(original, testVectorFileReference())
	if err != nil {
		t.Fatal(err)
	}
	source := &graphPreparedSource{values: [][]float32{{math.Float32frombits(1 << 31)}}, dimensions: 1, metric: vector.MetricL2Squared}
	if _, _, err := openGraph(bytes.NewReader(data), source, testVectorFileReference(), DefaultGraphLimits()); !errors.Is(err, ErrGraphVectorStore) {
		t.Fatalf("negative-zero source error = %v", err)
	}
}
