package semanticpersist

import "os"

type durabilityPolicy struct{ mode DurabilityMode }

func (d durabilityPolicy) synchronous() bool { return d.mode == DurabilitySynchronous }

func (d durabilityPolicy) syncFile(file *os.File) error {
	if !d.synchronous() {
		return nil
	}
	return file.Sync()
}

func (d durabilityPolicy) syncRegularFile(path string) error {
	if !d.synchronous() {
		return nil
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return ErrSymlink
	}
	if !info.Mode().IsRegular() {
		return ErrCorrupt
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func (d durabilityPolicy) syncDirectory(path string) error {
	if !d.synchronous() {
		return nil
	}
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
