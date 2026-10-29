package presence

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"

	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

type term struct {
	key    string
	count  uint64
	offset uint64
	length uint64
	single posting.Ord
}

// Reader retains only a sorted term dictionary. Independent cursors may be used
// concurrently. Its underlying ReaderAt must support concurrent reads and remain
// open and immutable while the Reader or any cursor is in use.
type Reader struct {
	source io.ReaderAt
	terms  []term
}

// Open references immutable data; it does not copy the underlying byte slice.
func Open(data []byte) (*Reader, error) {
	return OpenReaderAt(bytes.NewReader(data), int64(len(data)))
}

// FileReader owns the file opened by OpenFile. Close only after all queries finish.
type FileReader struct {
	*Reader
	file *os.File
}

// Close releases the file. Repeated calls and calls on a nil reader are harmless.
// Close must not run concurrently with queries or other Close calls.
func (r *FileReader) Close() error {
	if r == nil || r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}

// OpenFile opens a presence segment for file-backed queries and verifies its checksum.
func OpenFile(path string) (*FileReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("presence: open %q: %w", path, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("presence: stat %q: %w", path, err)
	}
	r, err := OpenReaderAt(file, info.Size())
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("presence: open %q: %w", path, err)
	}
	return &FileReader{Reader: r, file: file}, nil
}

// OpenReaderAt verifies the full checksum using bounded buffers, then loads the
// dictionary. Posting blocks are validated lazily when visited by a cursor.
func OpenReaderAt(source io.ReaderAt, size int64) (*Reader, error) {
	if source == nil || size < 4+1+footerSize {
		return nil, fmt.Errorf("presence: invalid source or size")
	}
	header, err := readAt(source, 0, 4)
	if err != nil {
		return nil, err
	}
	footer, err := readAt(source, size-footerSize, footerSize)
	if err != nil {
		return nil, err
	}
	if string(header) != magic || string(footer[20:]) != magic {
		return nil, fmt.Errorf("presence: invalid magic/version")
	}
	off := binary.LittleEndian.Uint64(footer)
	length := binary.LittleEndian.Uint64(footer[8:])
	end := uint64(size - footerSize)
	if off < 4 || off > end || length != end-off || length > uint64(math.MaxInt) {
		return nil, fmt.Errorf("presence: invalid dictionary range")
	}
	h := crc32.NewIEEE()
	if _, err := io.Copy(h, io.NewSectionReader(source, 0, int64(end))); err != nil {
		return nil, err
	}
	if h.Sum32() != binary.LittleEndian.Uint32(footer[16:]) {
		return nil, fmt.Errorf("presence: checksum mismatch")
	}
	dict, err := readAt(source, int64(off), int(length))
	if err != nil {
		return nil, err
	}
	count, err := takeUvarint(&dict)
	if err != nil || count > uint64(len(dict))/3 {
		return nil, fmt.Errorf("presence: invalid term count")
	}
	r := &Reader{source: source, terms: make([]term, 0, int(count))}
	for range count {
		n, err := takeUvarint(&dict)
		if err != nil || n == 0 || n > uint64(len(dict)) {
			return nil, fmt.Errorf("presence: invalid term length")
		}
		t := term{key: string(dict[:n])}
		dict = dict[n:]
		if len(r.terms) > 0 && t.key <= r.terms[len(r.terms)-1].key {
			return nil, fmt.Errorf("presence: unsorted dictionary")
		}
		t.count, err = takeUvarint(&dict)
		if err != nil || t.count == 0 || t.count > uint64(math.MaxUint32)+1 {
			return nil, fmt.Errorf("presence: invalid document frequency")
		}
		if t.count == 1 {
			ord, err := takeUvarint(&dict)
			if err != nil || ord > math.MaxUint32 {
				return nil, fmt.Errorf("presence: invalid singleton")
			}
			t.single = posting.Ord(ord)
		} else {
			var offsetErr, lengthErr error
			t.offset, offsetErr = takeUvarint(&dict)
			t.length, lengthErr = takeUvarint(&dict)
			minLength := ((t.count-1)/blockSize+1)*blockEntrySize + t.count
			if offsetErr != nil || lengthErr != nil || t.offset < 4 || t.offset > off ||
				t.length > off-t.offset || t.length < minLength || t.length > math.MaxUint32 {
				return nil, fmt.Errorf("presence: invalid posting range")
			}
		}
		r.terms = append(r.terms, t)
	}
	if len(dict) != 0 {
		return nil, fmt.Errorf("presence: trailing dictionary data")
	}
	return r, nil
}

func readAt(source io.ReaderAt, off int64, n int) ([]byte, error) {
	b := make([]byte, n)
	got, err := source.ReadAt(b, off)
	if got != n {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return b, nil
}

func takeUvarint(b *[]byte) (uint64, error) {
	v, n := binary.Uvarint(*b)
	if n <= 0 {
		return 0, fmt.Errorf("presence: invalid varint")
	}
	*b = (*b)[n:]
	return v, nil
}
