package fts

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
)

// KeyGeneratorDescriptor identifies the key-generation behavior persisted in
// an index.
type KeyGeneratorDescriptor struct {
	Name        string
	Version     uint32
	Fingerprint string
}

func NewKeyGeneratorDescriptor(name string, version uint32) KeyGeneratorDescriptor {
	spec := fmt.Sprintf("%s@%d", name, version)
	sum := sha256.Sum256([]byte(spec))
	return KeyGeneratorDescriptor{Name: name, Version: version, Fingerprint: hex.EncodeToString(sum[:])}
}

func defaultKeyGeneratorDescriptor(keyGen KeyGenerator) (KeyGeneratorDescriptor, bool) {
	if keyGen == nil || reflect.ValueOf(keyGen).Pointer() == reflect.ValueOf(WordKeys).Pointer() {
		return NewKeyGeneratorDescriptor("word-keys", 1), true
	}
	return KeyGeneratorDescriptor{}, false
}

func WordKeys(token string) ([]string, error) {
	return []string{token}, nil
}
