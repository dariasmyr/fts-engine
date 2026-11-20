package encoding

import "github.com/dariasmyr/fts-engine/v2/pkg/chunk"

func splitEncoderText(splitter ChunkSplitter, text string) ([]chunk.Range, error) {
	if splitter != nil {
		return splitter.Split(text)
	}
	whole, err := chunk.Whole(text)
	if err != nil {
		return nil, err
	}
	return []chunk.Range{whole}, nil
}
