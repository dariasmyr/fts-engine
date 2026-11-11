package hnswformat

// Config is the complete static validation configuration for persisted HNSW
// vectors and graph topology.
type Config struct {
	MaxFileBytes   uint64
	MaxGraphBytes  uint64
	MaxVectorBytes uint64
	MaxGraphLinks  uint64
	MaxDimensions  int
	MaxVectors     int
	MaxK           int
}

func (c Config) validate() error {
	if c.MaxFileBytes == 0 || c.MaxGraphBytes == 0 || c.MaxVectorBytes == 0 ||
		c.MaxGraphLinks == 0 || c.MaxDimensions <= 0 || c.MaxVectors <= 0 || c.MaxK <= 0 {
		return ErrLimitExceeded
	}
	return nil
}
