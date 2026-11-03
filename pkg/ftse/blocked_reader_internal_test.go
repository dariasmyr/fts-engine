package ftse

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

type readCountingReaderAt struct {
	reader *bytes.Reader
	read   int
}

func (r *readCountingReaderAt) ReadAt(data []byte, offset int64) (int, error) {
	n, err := r.reader.ReadAt(data, offset)
	r.read += n
	return n, err
}

type blockedTestMetadata struct {
	indexOff      int
	directoryOff  int
	directoryLen  int
	directoryCRC  int
	directoryVar  int
	directorySize int
}

func TestOpenV3RejectsInvalidHeaderMetadata(t *testing.T) {
	data := mustBuildBlockedTestSegment(t)
	binary.LittleEndian.PutUint16(data[8:10], postingBlockSize+1)
	if _, err := Open(data); err == nil {
		t.Fatal("Open() accepted an unsupported v3 block size")
	}
}

func TestOpenV3RejectsEveryTruncation(t *testing.T) {
	data := mustBuildBlockedTestSegment(t)
	for size := range len(data) {
		if _, err := Open(data[:size]); err == nil {
			t.Fatalf("Open(data[:%d]) accepted a truncated v3 segment", size)
		}
	}
}

func TestOpenReaderAtV3DoesNotScanDataPayload(t *testing.T) {
	postings := make([]posting.Posting, 100_000)
	for i := range postings {
		postings[i] = posting.Posting{Ord: posting.Ord(i), Count: 1}
	}
	data, err := Build([]posting.TermData{{Term: "common", Postings: postings}})
	if err != nil {
		t.Fatal(err)
	}
	backing := &readCountingReaderAt{reader: bytes.NewReader(data)}
	reader, err := OpenReaderAt(backing, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if backing.read >= len(data)/10 {
		t.Fatalf("OpenReaderAt() read %d of %d bytes", backing.read, len(data))
	}
	cursor, _, err := reader.OpenOrdinalPostings(context.Background(), "common")
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if !cursor.Advance(99_999) || cursor.Ord() != 99_999 {
		t.Fatalf("Advance() ord=%d err=%v", cursor.Ord(), cursor.Err())
	}
}

func TestOpenV3RejectsDictionaryDirectoryBounds(t *testing.T) {
	data := mustBuildBlockedTestSegment(t)
	metadata := locateBlockedTestMetadata(t, data)
	if metadata.directorySize != 1 {
		t.Fatalf("test requires one-byte directory offset, got %d", metadata.directorySize)
	}
	data[metadata.directoryVar] = 0
	refreshDictionaryChecksum(data, metadata.indexOff)
	if _, err := Open(data); err == nil {
		t.Fatal("Open() accepted an out-of-bounds directory")
	}
}

func TestV3DirectoryChecksumIsLazy(t *testing.T) {
	data := mustBuildBlockedTestSegment(t)
	metadata := locateBlockedTestMetadata(t, data)
	data[metadata.directoryOff] ^= 0xff
	reader, err := Open(data)
	if err != nil {
		t.Fatalf("Open() checked directory data eagerly: %v", err)
	}
	if _, err := reader.Search("alpha"); err == nil {
		t.Fatal("Search() accepted a corrupt directory")
	}
}

func TestV3DirectoryRejectsDataRangeIntoMetadata(t *testing.T) {
	data := mustBuildBlockedTestSegment(t)
	metadata := locateBlockedTestMetadata(t, data)
	directory := data[metadata.directoryOff : metadata.directoryOff+metadata.directoryLen]
	binary.LittleEndian.PutUint64(directory[8:16], uint64(metadata.directoryOff))
	binary.LittleEndian.PutUint32(data[metadata.directoryCRC:metadata.directoryCRC+4], crc32.ChecksumIEEE(directory))
	refreshDictionaryChecksum(data, metadata.indexOff)
	reader, err := Open(data)
	if err != nil {
		t.Fatalf("Open() checked directory entries eagerly: %v", err)
	}
	if _, err := reader.Search("alpha"); err == nil {
		t.Fatal("Search() accepted a posting range that points into metadata")
	}
}

func mustBuildBlockedTestSegment(t testing.TB) []byte {
	t.Helper()
	data, err := Build([]posting.TermData{{
		Term:     "alpha",
		Postings: []posting.Posting{{Ord: 1, Count: 1}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func locateBlockedTestMetadata(t *testing.T, data []byte) blockedTestMetadata {
	t.Helper()
	footer := data[len(data)-footerLen:]
	indexOff := int(binary.LittleEndian.Uint64(footer[0:8]))
	dictionary := data[indexOff : len(data)-footerLen]
	pos := 0
	take := func() uint64 {
		value, err := takeUvarint(dictionary, &pos)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	if count := take(); count != 1 {
		t.Fatalf("term count = %d, want 1", count)
	}
	termLen := int(take())
	pos += termLen
	_ = take() // document frequency
	directoryVarStart := pos
	directoryOff := int(take())
	directoryVarSize := pos - directoryVarStart
	directoryLen := int(take())
	return blockedTestMetadata{
		indexOff:      indexOff,
		directoryOff:  directoryOff,
		directoryLen:  directoryLen,
		directoryCRC:  indexOff + pos,
		directoryVar:  indexOff + directoryVarStart,
		directorySize: directoryVarSize,
	}
}

func refreshDictionaryChecksum(data []byte, indexOff int) {
	dictionary := data[indexOff : len(data)-footerLen]
	binary.LittleEndian.PutUint32(data[len(data)-footerLen+16:], crc32.ChecksumIEEE(dictionary))
}

func FuzzOpenV3(f *testing.F) {
	f.Add(mustBuildBlockedTestSegment(f))
	f.Add([]byte("FTSE\x03\x00"))
	f.Fuzz(func(t *testing.T, data []byte) {
		reader, err := Open(data)
		if err != nil {
			return
		}
		_ = reader.VerifyAll(context.Background())
	})
}
