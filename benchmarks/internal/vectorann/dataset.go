package vectorann

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand/v2"
)

type DatasetKind string

const (
	DatasetUniform   DatasetKind = "uniform"
	DatasetClustered DatasetKind = "clustered"
	DatasetDuplicate DatasetKind = "duplicate"

	SyntheticGeneratorVersion = "synthetic-phase6.v2"
)

type DatasetConfig struct {
	Kind              DatasetKind
	Dimensions        int
	Vectors           int
	Queries           int
	Seed              uint64
	Clusters          int
	ChunksPerDocument int
}

type Dataset struct {
	Config  DatasetConfig
	Vectors [][]float32
	Queries [][]float32
	Hash    string
}

func GenerateDataset(config DatasetConfig) (Dataset, error) {
	if config.Dimensions <= 0 || config.Vectors <= 0 || config.Queries <= 0 {
		return Dataset{}, fmt.Errorf("vectorann: dimensions, vectors, and queries must be positive")
	}
	if config.ChunksPerDocument == 0 {
		config.ChunksPerDocument = 1
	}
	if config.ChunksPerDocument < 1 {
		return Dataset{}, fmt.Errorf("vectorann: chunks per document must be positive")
	}
	if config.Kind == DatasetClustered {
		if config.Clusters == 0 {
			config.Clusters = 8
		}
		if config.Clusters < 1 {
			return Dataset{}, fmt.Errorf("vectorann: clusters must be positive")
		}
	} else {
		config.Clusters = 0
	}

	vectors := make([][]float32, config.Vectors)
	queries := make([][]float32, config.Queries)
	vectorRNG := newRNG(config.Seed, 0x766563746f7273)
	queryRNG := newRNG(config.Seed, 0x71756572696573)
	switch config.Kind {
	case DatasetUniform:
		fillUniform(vectorRNG, vectors, config.Dimensions)
		fillUniform(queryRNG, queries, config.Dimensions)
	case DatasetClustered:
		centers := make([][]float32, config.Clusters)
		fillUniform(newRNG(config.Seed, 0x63656e74657273), centers, config.Dimensions)
		fillClustered(vectorRNG, vectors, centers, 0.08)
		fillClustered(queryRNG, queries, centers, 0.08)
	case DatasetDuplicate:
		unique := max(1, config.Vectors/4)
		bases := make([][]float32, unique)
		fillUniform(vectorRNG, bases, config.Dimensions)
		for i := range vectors {
			vectors[i] = append([]float32(nil), bases[i%unique]...)
		}
		// Queries use an independent stream and are never selected from stored rows.
		fillUniform(queryRNG, queries, config.Dimensions)
	default:
		return Dataset{}, fmt.Errorf("vectorann: unknown dataset kind %q", config.Kind)
	}

	dataset := Dataset{Config: config, Vectors: vectors, Queries: queries}
	dataset.Hash = hashDataset(dataset)
	return dataset, nil
}

func newRNG(seed, stream uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed^stream, seed+stream+0x9e3779b97f4a7c15))
}

func fillUniform(rng *rand.Rand, rows [][]float32, dimensions int) {
	for row := range rows {
		rows[row] = make([]float32, dimensions)
		for dimension := range dimensions {
			rows[row][dimension] = rng.Float32()*2 - 1
		}
	}
}

func fillClustered(rng *rand.Rand, rows, centers [][]float32, sigma float64) {
	for row := range rows {
		center := centers[rng.IntN(len(centers))]
		rows[row] = make([]float32, len(center))
		for dimension := range center {
			rows[row][dimension] = center[dimension] + float32(rng.NormFloat64()*sigma)
		}
	}
}

func hashDataset(dataset Dataset) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(SyntheticGeneratorVersion + "\x00" + string(dataset.Config.Kind) + "\x00"))
	var encoded [8]byte
	for _, value := range []uint64{
		uint64(dataset.Config.Dimensions), uint64(dataset.Config.Vectors), uint64(dataset.Config.Queries),
		dataset.Config.Seed, uint64(dataset.Config.Clusters), uint64(dataset.Config.ChunksPerDocument),
	} {
		binary.LittleEndian.PutUint64(encoded[:], value)
		_, _ = hash.Write(encoded[:])
	}
	for _, rows := range [][][]float32{dataset.Vectors, dataset.Queries} {
		for _, row := range rows {
			for _, value := range row {
				binary.LittleEndian.PutUint32(encoded[:4], math.Float32bits(value))
				_, _ = hash.Write(encoded[:4])
			}
		}
	}
	return hex.EncodeToString(hash.Sum(nil))
}
