package semanticpersist

import (
	"crypto/sha256"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/v2/internal/persistfs"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
)

func prepareLayout(l layout) (bool, error) {
	rootCreated := false
	if info, err := os.Lstat(l.root); errors.Is(err, os.ErrNotExist) {
		if err := validateDirectory(filepath.Dir(l.root)); err != nil {
			return false, err
		}
		if err := os.Mkdir(l.root, 0o700); err != nil {
			return false, err
		}
		rootCreated = true
	} else if err != nil {
		return false, err
	} else if info.Mode()&os.ModeSymlink != 0 {
		return false, ErrSymlink
	} else if !info.IsDir() {
		return false, ErrCorrupt
	}
	for _, path := range []string{l.root, l.objects, l.segments, l.generations} {
		if err := ensureDirectory(path); err != nil {
			return false, err
		}
	}
	return rootCreated, nil
}

func validateLayout(l layout) error {
	for _, path := range []string{l.root, l.objects, l.segments, l.generations} {
		if err := validateDirectory(path); err != nil {
			return err
		}
	}
	return nil
}

func ensureDirectory(path string) error {
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return validateDirectory(path)
}

func validateDirectory(path string) error {
	return mapPersistFSError(persistfs.ValidateDirectory(path), ErrCorrupt)
}

func ensureContained(root, path string) error {
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == ".." || filepath.IsAbs(relative) ||
		(len(relative) >= 3 && relative[:3] == ".."+string(filepath.Separator)) {
		return ErrPathEscape
	}
	return nil
}

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

func verifyReferencedFile(path string, ref generationformat.FileRef, limit uint64, syncFile bool) error {
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
	if copyErr == nil && syncFile {
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

func writeAllFile(writer io.Writer, data []byte) error {
	return persistfs.WriteAll(writer, data)
}

func writeDataFile(path string, data []byte, durability durabilityPolicy) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	writeErr := writeAllFile(file, data)
	if writeErr == nil {
		writeErr = durability.syncFile(file)
	}
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func writeTemporaryData(dir, pattern string, data []byte, durability durabilityPolicy) (string, error) {
	return persistfs.WriteTempFile(
		dir,
		pattern,
		persistfs.TempFileOptions{Sync: durability.synchronous()},
		func(writer io.Writer) error { return persistfs.WriteAll(writer, data) },
	)
}

func mapPersistFSError(err, invalidType error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, persistfs.ErrSymlink):
		return ErrSymlink
	case errors.Is(err, persistfs.ErrLimitExceeded):
		return ErrLimitExceeded
	case errors.Is(err, persistfs.ErrNotDirectory), errors.Is(err, persistfs.ErrNotRegular):
		return invalidType
	default:
		return err
	}
}
