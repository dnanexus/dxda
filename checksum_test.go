package dxda

import "testing"

func TestCRC64NVME(t *testing.T) {
	data := []byte("The quick brown fox jumps over the lazy dog")
	expectedChecksum := "12xUBUlUwUM="

	checksum, err := CalculateChecksum("CRC64NVME", data)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if checksum != expectedChecksum {
		t.Errorf("Expected checksum %s, got %s", expectedChecksum, checksum)
	}
}

func TestCRC32C(t *testing.T) {
	data := []byte("The quick brown fox jumps over the lazy dog")
	expectedChecksum := "ImIEBA=="

	checksum, err := CalculateChecksum("CRC32C", data)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if checksum != expectedChecksum {
		t.Errorf("Expected checksum %s, got %s", expectedChecksum, checksum)
	}
}

func TestCRC32(t *testing.T) {
	data := []byte("The quick brown fox jumps over the lazy dog")
	expectedChecksum := "QU+jOQ=="

	checksum, err := CalculateChecksum("CRC32", data)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if checksum != expectedChecksum {
		t.Errorf("Expected checksum %s, got %s", expectedChecksum, checksum)
	}
}

func TestSHA256(t *testing.T) {
	data := []byte("The quick brown fox jumps over the lazy dog")
	expectedChecksum := "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592"

	checksum, err := CalculateChecksum("SHA256", data)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if checksum != expectedChecksum {
		t.Errorf("Expected checksum %s, got %s", expectedChecksum, checksum)
	}
}

func TestSHA1(t *testing.T) {
	data := []byte("The quick brown fox jumps over the lazy dog")
	expectedChecksum := "2fd4e1c67a2d28fced849ee1bb76e7391b93eb12"

	checksum, err := CalculateChecksum("SHA1", data)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if checksum != expectedChecksum {
		t.Errorf("Expected checksum %s, got %s", expectedChecksum, checksum)
	}
}

func TestUnsupportedChecksum(t *testing.T) {
	data := []byte("The quick brown fox jumps over the lazy dog")

	_, err := CalculateChecksum("MD5", data)
	if err == nil {
		t.Fatalf("Expected error for unsupported checksum type, got nil")
	}
}
