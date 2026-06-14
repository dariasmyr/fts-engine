//go:build !linux && !darwin && !freebsd

package segment

import "os"

func openMmap(path string) ([]byte, func() error, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	return data, func() error { return nil }, nil
}
