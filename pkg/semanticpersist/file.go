package semanticpersist

import (
	"crypto/sha256"
	"errors"
	"io"
	"math"
	"os"

	"github.com/dariasmyr/fts-engine/v2/internal/persistfs"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
)

func readRegularFile(path string, limit uint64) ([]byte, error) {
	data, err := persistfs.ReadRegularFile(path, limit)
	return data, mapPersistFSError(err, ErrLimitExceeded)
}

func readReferencedFile(path string, ref generationformat.FileRef, limit uint64) ([]byte, error) {
	return readReferencedFileData(path, ref, limit, true)
}

func readReferencedFileWithoutHash(path string, ref generationformat.FileRef, limit uint64) ([]byte, error) {
	return readReferencedFileData(path, ref, limit, false)
}

func readReferencedFileData(path string, ref generationformat.FileRef, limit uint64, verifyHash bool) ([]byte, error) {
	if ref.Size == 0 || ref.Size > limit || ref.Size >= math.MaxInt64 {
		return nil, ErrLimitExceeded
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, corruptMissingReference(err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrSymlink
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != ref.Size {
		return nil, ErrCorrupt
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, corruptMissingReference(err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, int64(ref.Size)+1))
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) != ref.Size || (verifyHash && sha256.Sum256(data) != ref.SHA256) {
		return nil, ErrCorrupt
	}
	return data, nil
}

func verifyReferencedFile(path string, ref generationformat.FileRef, limit uint64) error {
	if ref.Size == 0 || ref.Size > limit || ref.Size >= math.MaxInt64 {
		return ErrLimitExceeded
	}
	info, err := os.Lstat(path)
	if err != nil {
		return corruptMissingReference(err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return ErrSymlink
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != ref.Size {
		return ErrCorrupt
	}
	file, err := os.Open(path)
	if err != nil {
		return corruptMissingReference(err)
	}
	hash := sha256.New()
	buffer := make([]byte, 64<<10)
	written, copyErr := io.CopyBuffer(hash, io.LimitReader(file, int64(ref.Size)+1), buffer)
	if copyErr == nil && (uint64(written) != ref.Size || !equalSHA256(hash.Sum(nil), ref.SHA256)) {
		copyErr = ErrCorrupt
	}
	if copyErr == nil {
		copyErr = file.Sync()
	}
	closeErr := file.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func corruptMissingReference(err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return ErrCorrupt
	}
	return err
}

func equalSHA256(sum []byte, expected [sha256.Size]byte) bool {
	if len(sum) != sha256.Size {
		return false
	}
	var actual [sha256.Size]byte
	copy(actual[:], sum)
	return actual == expected
}

func writeDataFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	writeErr := persistfs.WriteAll(file, data)
	if writeErr == nil {
		writeErr = persistfs.SyncFile(file)
	}
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func writeTemporaryData(dir, pattern string, data []byte) (string, error) {
	return persistfs.WriteTempFile(
		dir,
		pattern,
		persistfs.TempFileOptions{Sync: true},
		func(writer io.Writer) error { return persistfs.WriteAll(writer, data) },
	)
}
