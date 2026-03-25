package fts

import "fmt"

type ContainsChecker interface {
	Contains(item []byte) bool
}

func NormalizeToKeys(text string, pipeline Pipeline, keyGen KeyGenerator) ([]string, error) {
	if pipeline == nil {
		pipeline = defaultPipeline{}
	}
	if keyGen == nil {
		keyGen = WordKeys
	}

	tokens := pipeline.Process(text)
	keys := make([]string, 0, len(tokens))

	for _, token := range tokens {
		generated, err := keyGen(token)
		if err != nil {
			return nil, fmt.Errorf("fts: normalize keys: keygen: %w", err)
		}
		keys = append(keys, generated...)
	}

	return keys, nil
}

func ContainsNormalized(searchFilter ContainsChecker, text string, pipeline Pipeline, keyGen KeyGenerator) (bool, error) {
	if searchFilter == nil {
		return true, nil
	}

	keys, err := NormalizeToKeys(text, pipeline, keyGen)
	if err != nil {
		return false, err
	}

	if len(keys) == 0 {
		return false, nil
	}

	for _, key := range keys {
		if !searchFilter.Contains([]byte(key)) {
			return false, nil
		}
	}

	return true, nil
}
