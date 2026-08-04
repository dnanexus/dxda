package dxda

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"

	"github.com/minio/crc64nvme"
)

const (
	ChecksumCRC64NVME = "CRC64NVME"
	ChecksumCRC32C    = "CRC32C"
	ChecksumCRC32     = "CRC32"
	ChecksumSHA256    = "SHA256"
	ChecksumSHA1      = "SHA1"
)

// A streaming hasher for one of the supported checksum types. It pairs the
// hash with the textual encoding used for that checksum type: the CRC
// variants are represented in base64, the SHA variants in hex.
type ChecksumHasher struct {
	hash.Hash
	encode func([]byte) string
}

// Create a streaming hasher for the given checksum type. Streaming lets us
// verify a file part without holding it in memory, which matters because a
// part can be several hundred megabytes and many are checked in parallel.
func NewChecksumHasher(checksumType string) (*ChecksumHasher, error) {
	switch checksumType {
	case ChecksumCRC64NVME:
		return &ChecksumHasher{crc64nvme.New(), base64.StdEncoding.EncodeToString}, nil
	case ChecksumCRC32C:
		// Castagnoli polynomial is used in CRC32C standard
		return &ChecksumHasher{crc32.New(crc32.MakeTable(crc32.Castagnoli)), base64.StdEncoding.EncodeToString}, nil
	case ChecksumCRC32:
		// IEEE polynomial is used in CRC32 standard
		return &ChecksumHasher{crc32.New(crc32.MakeTable(crc32.IEEE)), base64.StdEncoding.EncodeToString}, nil
	case ChecksumSHA256:
		return &ChecksumHasher{sha256.New(), hex.EncodeToString}, nil
	case ChecksumSHA1:
		return &ChecksumHasher{sha1.New(), hex.EncodeToString}, nil
	default:
		return nil, fmt.Errorf("unsupported checksum type: %s", checksumType)
	}
}

// The checksum of everything written to the hasher so far, in the textual
// representation used by the platform for this checksum type.
func (h *ChecksumHasher) Checksum() string {
	return h.encode(h.Sum(nil))
}

// Calculate the checksum of an in-memory buffer. Prefer NewChecksumHasher for
// anything large enough that we do not want to hold it in memory.
func CalculateChecksum(checksumType string, data []byte) (string, error) {
	hasher, err := NewChecksumHasher(checksumType)
	if err != nil {
		return "", err
	}
	if _, err := hasher.Write(data); err != nil {
		return "", err
	}
	return hasher.Checksum(), nil
}
