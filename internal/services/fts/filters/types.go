package filters

type Filter interface {
	Add(key string)
	MightContain(key string) bool
}

type BloomConfig struct {
	Capacity uint64
	Hashes   uint64
}

type CuckooConfig struct {
	Capacity uint64
	BucketSz int
	MaxKicks int
}

type RibbonConfig struct {
	Bits  uint64
	Width uint64
}
