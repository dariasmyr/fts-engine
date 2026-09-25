package persist

import (
	"errors"
	"io"
)

var ErrLimitExceeded = errors.New("persist: configured limit exceeded")

// ReadBounded reads at most limit bytes and rejects larger input.
func ReadBounded(reader io.Reader, limit uint64) ([]byte, error) {
	if reader == nil || limit == 0 || limit >= uint64(^uint64(0)>>1) {
		return nil, ErrLimitExceeded
	}
	data, err := io.ReadAll(io.LimitReader(reader, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) > limit {
		return nil, ErrLimitExceeded
	}
	return data, nil
}
