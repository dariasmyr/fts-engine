package hnswformat

import (
	"io"

	"github.com/dariasmyr/fts-engine/v2/internal/format"
)

// FileRef identifies an immutable persisted file by size and SHA-256.
type FileRef = format.FileRef

// FileMetadata describes one encoded segment file.
type FileMetadata struct {
	FileRef
	CRC32 uint32
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
