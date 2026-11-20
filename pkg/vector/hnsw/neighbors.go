package hnsw

import (
	"context"
	"slices"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

const maxVerifiedLinkWords = (maxSupportedNeighbors*2 + 63) / 64

type neighborSelection struct {
	neighbors      []buildNeighbor
	verifiedCount  int
	distanceChecks uint64
	skippedChecks  uint64
}

func (b *builder) distanceNodes(a, c nodeOrdinal) float64 {
	distance, err := b.matrix.DistanceRows(vector.Ordinal(a), vector.Ordinal(c))
	if err != nil {
		panic("vector/hnsw: builder referenced an invalid matrix row")
	}
	return distance
}

func (b *builder) selectClosestNeighbors(ctx context.Context, candidates []candidate, limit int) ([]buildNeighbor, error) {
	selection, err := b.selectNeighbors(ctx, candidates, limit, true, nil)
	return selection.neighbors, err
}

func (b *builder) selectNeighbors(
	ctx context.Context,
	candidates []candidate,
	limit int,
	fillFromRejected bool,
	selectedBuffer []buildNeighbor,
) (neighborSelection, error) {
	// The returned neighbors reuse selectedBuffer when it has enough capacity.
	// Callers that install the result in the graph therefore transfer that
	// buffer to the graph; rejected candidates remain builder scratch.
	// sort candidates by distance, then by node ordinal
	if err := b.sortNeighbors(ctx, candidates); err != nil {
		return neighborSelection{}, err
	}
	invalidateDuplicateVerification(candidates)
	selectedCapacity := 0
	if len(candidates) > 0 {
		selectedCapacity = limit
	}
	selected := prepareEmptyBuffer(selectedBuffer, selectedCapacity)
	var rejected []buildNeighbor
	if fillFromRejected {
		rejected = prepareEmptyBuffer(b.workspace.rejectedNeighbors, len(candidates))
		defer func() {
			b.workspace.rejectedNeighbors = rejected[:0]
		}()
	}
	// temporary bitmap to track which existing neighbors have been verified, to avoid redundant distance checks
	var previouslyVerified [maxVerifiedLinkWords]uint64

	var distanceChecks, skippedChecks uint64
	// reject candidates already represented by a selected, closer neighbor. This
	// preserves links into different regions instead of only the nearest cluster.
	work := 0
	for _, candidate := range candidates {
		if err := periodicContextError(ctx, work); err != nil {
			return neighborSelection{}, err
		}
		work++

		// a diverse candidate is a candidate
		// that is not closer to any of the already selected neighbors than it is to the target node
		diverse := true
		for existingIndex, existing := range selected {
			if err := periodicContextError(ctx, work); err != nil {
				return neighborSelection{}, err
			}
			work++
			// this pair already passed diversification during an earlier neighbor selection
			// for the same target node and level.
			// the vectors do not change during build, so repeating the distance check
			// would give the same result.
			if candidate.verified && verifiedBit(previouslyVerified[:], existingIndex) {
				skippedChecks++
				continue
			}
			distanceChecks++
			// if current candidate closer to the existing neighbor than the target node, reject it
			if b.distanceNodes(candidate.node, existing.node) < candidate.distance {
				diverse = false
				break
			}
		}
		if diverse {
			if candidate.verified {
				setVerifiedBit(previouslyVerified[:], len(selected))
			}
			selected = append(selected, buildNeighbor{
				node:     candidate.node,
				distance: candidate.distance,
			})
			if len(selected) == limit {
				return neighborSelection{
					neighbors:      selected,
					verifiedCount:  len(selected),
					distanceChecks: distanceChecks,
					skippedChecks:  skippedChecks,
				}, ctx.Err()
			}
		} else {
			if fillFromRejected {
				rejected = append(rejected, buildNeighbor{
					node:     candidate.node,
					distance: candidate.distance,
				})
			}
		}
	}
	verifiedCount := len(selected)
	// fill unused slots only after choosing diverse neighbors. This may
	// increase graph degree and recall, at the cost of more traversed links.
	for i, neighbor := range rejected {
		if err := periodicContextError(ctx, i); err != nil {
			return neighborSelection{}, err
		}
		if len(selected) == limit {
			break
		}
		selected = append(selected, neighbor)
	}
	return neighborSelection{
		neighbors:      selected,
		verifiedCount:  verifiedCount,
		distanceChecks: distanceChecks,
		skippedChecks:  skippedChecks,
	}, ctx.Err()
}

func invalidateDuplicateVerification(candidates []candidate) {
	// Verification is attached to a link occurrence. Equal node ordinals are
	// indistinguishable after sorting, so recompute their checks conservatively.
	for startDuplicateNode := 0; startDuplicateNode < len(candidates); {
		endDuplicateNode := startDuplicateNode + 1
		for endDuplicateNode < len(candidates) && candidates[endDuplicateNode].node == candidates[startDuplicateNode].node {
			endDuplicateNode++ // extend the duplicate range
		}

		if endDuplicateNode-startDuplicateNode > 1 {
			for i := startDuplicateNode; i < endDuplicateNode; i++ {
				candidates[i].verified = false
			}
		}
		startDuplicateNode = endDuplicateNode
	}
}

func (b *builder) sortNeighbors(ctx context.Context, candidates []candidate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// sort candidates by distance, then by node ordinal
	slices.SortFunc(candidates, func(a, c candidate) int {
		if a.distance < c.distance {
			return -1
		}
		if a.distance > c.distance {
			return 1
		}
		if a.node < c.node {
			return -1
		}
		if a.node > c.node {
			return 1
		}
		return 0
	})
	return ctx.Err()
}

// addReverseLink adds existingNode → newNode. If existingNode is full, its
// links are selected again using distances from existingNode.
func (b *builder) addReverseLink(ctx context.Context, existingNode, newNode nodeOrdinal, level int) error {
	existingLinks := b.graph.nodes[existingNode].links[level]
	candidates := prepareEmptyBuffer(b.workspace.reverseLinkCandidates, len(existingLinks)+1)
	for i, link := range existingLinks {
		if err := periodicContextError(ctx, i); err != nil {
			return err
		}
		candidates = append(candidates, candidate{
			node:     link.node,
			distance: link.distance,
			verified: b.linkVerified(existingNode, level, i),
		})
	}
	candidates = append(candidates, candidate{node: newNode, distance: b.distanceNodes(existingNode, newNode)})
	// the maximum number of links allowed for a node is determined by the level
	// base level (level 0) allows for twice as many links as higher levels
	limit := b.config.MaxNeighbors
	if level == 0 {
		limit *= 2
	}
	closestNeighbors := existingLinks
	verifiedCount := 0
	// if the number of candidates exceeds the limit, we select the closest neighbors
	if len(candidates) > limit {
		// The old links are already copied into candidates. Selection can safely
		// overwrite their backing array while building the replacement list.
		selection, err := b.selectNeighbors(ctx, candidates, limit, false, existingLinks)
		if err != nil {
			return err
		}
		closestNeighbors = selection.neighbors
		verifiedCount = selection.verifiedCount
		b.workspace.reverseDistanceChecks += selection.distanceChecks
		b.workspace.reverseSkippedChecks += selection.skippedChecks
	} else {
		// if the number of candidates is within the limit, we sort them and keep all
		if err := b.sortNeighbors(ctx, candidates); err != nil {
			return err
		}
		if cap(closestNeighbors) < limit {
			// Reserve the complete level capacity once. Repeated exact-size growth
			// here previously reallocated the same node's links for every reverse
			// edge added before it reached the level limit.
			closestNeighbors = make([]buildNeighbor, len(candidates), limit)
		} else {
			closestNeighbors = closestNeighbors[:len(candidates)]
		}
		// Preserve each candidate's owner-relative distance in the mutable graph.
		for i := range candidates {
			closestNeighbors[i] = buildNeighbor{
				node:     candidates[i].node,
				distance: candidates[i].distance,
			}
		}
	}
	b.graph.nodes[existingNode].links[level] = closestNeighbors
	if len(candidates) > limit {
		b.setVerifiedPrefix(existingNode, level, verifiedCount)
	} else {
		b.setVerifiedCandidates(existingNode, level, candidates)
	}
	b.workspace.reverseLinkCandidates = candidates[:0]
	return ctx.Err()
}

func (b *builder) linkVerified(node nodeOrdinal, level, linkIndex int) bool {
	return verifiedBit(b.verifiedLinkBitsAtLevel(node, level), linkIndex)
}

func (b *builder) setVerifiedPrefix(node nodeOrdinal, level, count int) {
	bits := b.ensureVerifiedLinkBitsAtLevel(node, level)
	clear(bits)
	for index := 0; index < count; index++ {
		setVerifiedBit(bits, index)
	}
}

func (b *builder) setVerifiedCandidates(node nodeOrdinal, level int, candidates []candidate) {
	bits := b.ensureVerifiedLinkBitsAtLevel(node, level)
	clear(bits)
	for index, candidate := range candidates {
		if candidate.verified {
			setVerifiedBit(bits, index)
		}
	}
}

func (b *builder) clearVerifiedLinks(node nodeOrdinal, level int) {
	clear(b.verifiedLinkBitsAtLevel(node, level))
}

func (b *builder) ensureVerifiedLinkBitsAtLevel(node nodeOrdinal, level int) []uint64 {
	bits := b.verifiedLinkBitsAtLevel(node, level)
	if len(bits) != 0 {
		return bits
	}
	mutable := &b.graph.nodes[node]
	mutable.verifiedLinkBits = make([]uint64, b.verifiedLinkStorageWords(int(mutable.level)))
	return b.verifiedLinkBitsAtLevel(node, level)
}

func (b *builder) verifiedLinkBitsAtLevel(node nodeOrdinal, level int) []uint64 {
	bits := b.graph.nodes[node].verifiedLinkBits
	offset := 0
	if level > 0 {
		offset = b.verifiedLinkWordCount(0) + (level-1)*b.verifiedLinkWordCount(1)
	}
	end := offset + b.verifiedLinkWordCount(level)
	if end > len(bits) {
		return nil
	}
	return bits[offset:end]
}

func (b *builder) verifiedLinkStorageWords(level int) int {
	return b.verifiedLinkWordCount(0) + level*b.verifiedLinkWordCount(1)
}

func (b *builder) verifiedLinkWordCount(level int) int {
	limit := b.config.MaxNeighbors
	if level == 0 {
		limit *= 2
	}
	return (limit + 63) / 64
}

func verifiedBit(words []uint64, index int) bool {
	word := index / 64
	return word < len(words) && words[word]&(uint64(1)<<uint(index%64)) != 0
}

func setVerifiedBit(words []uint64, index int) {
	words[index/64] |= uint64(1) << uint(index%64)
}
