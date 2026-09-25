package hnsw

import "math"

const splitMixIncrement = uint64(0x9e3779b97f4a7c15)

// levelRNG is repository-owned SplitMix64 state. Its output and float mapping
// are part of LevelGeneratorVersion.
type levelRNG struct {
	state uint64
}

func newLevelRNG(seed uint64) levelRNG { return levelRNG{state: seed} }

func (r *levelRNG) nextUint64() uint64 {
	r.state += splitMixIncrement
	value := r.state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func (r *levelRNG) level(maxNeighbors int) uint8 {
	// The top 53 bits map exactly to one of 2^53 values in (0, 1].
	bits := r.nextUint64() >> 11
	u := float64(bits+1) / float64(uint64(1)<<53)
	return levelFromUnit(u, maxNeighbors)
}

func levelFromUnit(u float64, maxNeighbors int) uint8 {
	level := int(math.Floor(-math.Log(u) / math.Log(float64(maxNeighbors))))
	return uint8(min(level, MaxLevel))
}
