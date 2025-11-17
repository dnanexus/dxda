package dxda_test

import (
	"testing"

	"github.com/dnanexus/dxda"
)

func TestDBPartRegular_HandlesEmptyMD5(t *testing.T) {
	// Test that DBPartRegular struct can handle empty MD5 field
	part := dxda.DBPartRegular{
		FileId:           "test-file",
		Project:          "test-project",
		FileName:         "test.txt",
		Folder:           "test-folder",
		PartId:           1,
		Offset:           0,
		Size:             1024,
		MD5:              "", // Empty MD5 - this should be handled gracefully
		BytesFetched:     0,
		DownloadDoneTime: 0,
	}

	// Verify the part can be created with empty MD5
	if part.MD5 != "" {
		t.Errorf("Expected empty MD5, got: %s", part.MD5)
	}

	// Verify other fields are set correctly
	if part.FileId != "test-file" {
		t.Errorf("Expected FileId 'test-file', got: %s", part.FileId)
	}

	if part.Size != 1024 {
		t.Errorf("Expected Size 1024, got: %d", part.Size)
	}
}

func TestDBPartRegular_HandlesValidMD5(t *testing.T) {
	// Test that DBPartRegular struct works with valid MD5
	validMD5 := "5d41402abc4b2a76b9719d911017c592"

	part := dxda.DBPartRegular{
		FileId:           "test-file",
		Project:          "test-project",
		FileName:         "test.txt",
		Folder:           "test-folder",
		PartId:           1,
		Offset:           0,
		Size:             1024,
		MD5:              validMD5,
		BytesFetched:     0,
		DownloadDoneTime: 0,
	}

	// Verify the MD5 is set correctly
	if part.MD5 != validMD5 {
		t.Errorf("Expected MD5 '%s', got: %s", validMD5, part.MD5)
	}
}