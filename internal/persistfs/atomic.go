package persistfs

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type TempFileOptions struct {
	Sync           bool
	BufferSize     int
	FlushThreshold int
}

// WriteTempFile writes and closes a temporary file in dir. The caller owns the
// returned path and must either rename or remove it.
func WriteTempFile(dir, pattern string, options TempFileOptions, write func(io.Writer) error) (string, error) {
	if write == nil {
		return "", fmt.Errorf("persistfs: nil writer callback")
	}
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", fmt.Errorf("persistfs: create temp file: %w", err)
	}
	path := file.Name()
	owned := true
	defer func() {
		if owned {
			_ = os.Remove(path)
		}
	}()

	var writer io.Writer = file
	var buffered *thresholdBufferedWriter
	if options.BufferSize > 0 {
		buffered = newThresholdBufferedWriter(file, options.BufferSize, options.FlushThreshold)
		writer = buffered
	}
	if err := write(writer); err != nil {
		_ = file.Close()
		return "", fmt.Errorf("persistfs: write temp file: %w", err)
	}
	if buffered != nil {
		if err := buffered.Flush(); err != nil {
			_ = file.Close()
			return "", fmt.Errorf("persistfs: flush temp file: %w", err)
		}
	}
	if options.Sync {
		if err := SyncFile(file); err != nil {
			_ = file.Close()
			return "", fmt.Errorf("persistfs: sync temp file: %w", err)
		}
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("persistfs: close temp file: %w", err)
	}
	owned = false
	return path, nil
}

func Replace(source, destination string) error {
	return os.Rename(source, destination)
}

type thresholdBufferedWriter struct {
	writer         *bufio.Writer
	flushThreshold int
	pending        int
}

func newThresholdBufferedWriter(writer io.Writer, bufferSize, flushThreshold int) *thresholdBufferedWriter {
	return &thresholdBufferedWriter{
		writer:         bufio.NewWriterSize(writer, bufferSize),
		flushThreshold: flushThreshold,
	}
}

func (w *thresholdBufferedWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	w.pending += n
	if err != nil {
		return n, err
	}
	if w.flushThreshold > 0 && w.pending >= w.flushThreshold {
		if err := w.writer.Flush(); err != nil {
			return n, err
		}
		w.pending = 0
	}
	return n, nil
}

func (w *thresholdBufferedWriter) Flush() error {
	w.pending = 0
	return w.writer.Flush()
}
