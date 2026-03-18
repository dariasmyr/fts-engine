package keygen

import "errors"

var ErrInvalidTrigramSize = errors.New("trigram must have at least 3 characters")

func Trigram(token string) ([]string, error) {
	if len(token) < 3 {
		return nil, ErrInvalidTrigramSize
	}

	keys := make([]string, 0, len(token)-2)
	for i := 0; i < len(token)-2; i++ {
		keys = append(keys, token[i:i+3])
	}

	return keys, nil
}
