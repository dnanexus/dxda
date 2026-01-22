package dxda

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
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

func CalculateChecksum(checksumType string, data []byte) (string, error) {
	switch checksumType {
	case ChecksumCRC64NVME:
		return calculateCRC64NVME(data), nil
	case ChecksumCRC32C:
		return calculateCRC32C(data), nil
	case ChecksumCRC32:
		return calculateCRC32(data), nil
	case ChecksumSHA256:
		return calculateSHA256(data), nil
	case ChecksumSHA1:
		return calculateSHA1(data), nil
	default:
		return "", fmt.Errorf("unsupported checksum type: %s", checksumType)
	}
}

func calculateCRC64NVME(data []byte) string {
	return uint64ToBase64String(crc64nvme.Checksum(data))
}

func calculateCRC32C(data []byte) string {
	// Castagnoli polynomial is used in CRC32C standard
	table := crc32.MakeTable(crc32.Castagnoli)
	return uint32ToBase64String(crc32.Checksum(data, table))
}

func calculateCRC32(data []byte) string {
	// IEEE polynomial is used in CRC32 standard
	table := crc32.MakeTable(crc32.IEEE)
	return uint32ToBase64String(crc32.Checksum(data, table))
}

func calculateSHA256(data []byte) string {
	hasher := sha256.New()
	hasher.Write(data)
	return hex.EncodeToString(hasher.Sum(nil))
}

func calculateSHA1(data []byte) string {
	hasher := sha1.New()
	hasher.Write(data)
	return hex.EncodeToString(hasher.Sum(nil))
}

func uint64ToBase64String(num uint64) string {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, num)

	return base64.StdEncoding.EncodeToString(buf)
}

func uint32ToBase64String(num uint32) string {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, num)

	return base64.StdEncoding.EncodeToString(buf)
}
