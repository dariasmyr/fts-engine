package format

import (
	"encoding/binary"
	"hash/crc32"
)

const CRC32Size = 4

// CRC32 returns the IEEE CRC32 used by the repository's binary formats.
func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// PutCRC32 writes a little-endian CRC32 footer into dst.
func PutCRC32(dst []byte, checksum uint32) bool {
	if len(dst) < CRC32Size {
		return false
	}
	binary.LittleEndian.PutUint32(dst[:CRC32Size], checksum)
	return true
}

// ReadCRC32 reads a little-endian CRC32 footer from src.
func ReadCRC32(src []byte) (uint32, bool) {
	if len(src) < CRC32Size {
		return 0, false
	}
	return binary.LittleEndian.Uint32(src[:CRC32Size]), true
}
