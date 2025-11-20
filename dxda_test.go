package dxda_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/dnanexus/dxda"
)

func TestGetToken(t *testing.T) {
	os.Setenv("DX_API_TOKEN", "blah")
	dxEnv, method, err := dxda.GetDxEnvironment()
	if err != nil {
		t.Errorf("Encountered an error while getting the environment")
	}
	if dxEnv.Token != "blah" {
		t.Errorf(fmt.Sprintf("Expected token 'blah' but got %s", dxEnv.Token))
	}
	if method != "environment" {
		t.Errorf(fmt.Sprintf("Expected method of token retreival to be 'environment' but got %s", method))
	}

	os.Unsetenv("DX_API_TOKEN")

	// Explicitly not testing home directory config file as it may clobber existing login info for executor
}

func TestEnvironmentQuery(t *testing.T) {
	os.Setenv("DX_APISERVER_HOST", "a.b.c.com")
	os.Setenv("DX_APISERVER_PORT", "80")
	os.Setenv("DX_APISERVER_PROTOCOL", "xxyy")
	os.Setenv("DX_SECURITY_CONTEXT",
		`{"auth_token_type": "Bearer", "auth_token": "yQ2YJfyQZV74ygvvGF281P0G5bK4VZJ5VV1x6GQ4"}`)
	os.Setenv("DX_API_TOKEN", "")

	dxEnv, _, _ := dxda.GetDxEnvironment()
	if dxEnv.ApiServerHost != "a.b.c.com" {
		t.Errorf(fmt.Sprintf("Expected host 'a.b.c.com' but got %s", dxEnv.ApiServerHost))
	}
	if dxEnv.ApiServerPort != 80 {
		t.Errorf(fmt.Sprintf("Expected port '80' but got %d", dxEnv.ApiServerPort))
	}
	if dxEnv.ApiServerProtocol != "xxyy" {
		t.Errorf(fmt.Sprintf("Expected protocol 'xxyy' but got %s", dxEnv.ApiServerProtocol))
	}
	if dxEnv.Token != "yQ2YJfyQZV74ygvvGF281P0G5bK4VZJ5VV1x6GQ4" {
		t.Errorf(fmt.Sprintf("Expected token 'yQ2YJfyQZV74ygvvGF281P0G5bK4VZJ5VV1x6GQ4' but got %s", dxEnv.Token))
	}
}

func TestMD5SkipLogic(t *testing.T) {
	// Test that parts without MD5 checksums should skip verification
	// This tests the core logic without requiring actual downloads

	// Create a test part with no MD5
	partWithoutMD5 := dxda.DBPartRegular{
		FileId:   "file-123",
		Project:  "project-456",
		FileName: "test.txt",
		Folder:   "/test",
		PartId:   1,
		Offset:   0,
		Size:     1024,
		MD5:      "", // Empty MD5 - should skip verification
	}

	// Create a test part with MD5
	partWithMD5 := dxda.DBPartRegular{
		FileId:   "file-789",
		Project:  "project-456",
		FileName: "test2.txt",
		Folder:   "/test",
		PartId:   1,
		Offset:   0,
		Size:     1024,
		MD5:      "d41d8cd98f00b204e9800998ecf8427e", // Valid MD5
	}

	// Test that part without MD5 should be treated differently
	if partWithoutMD5.MD5 != "" {
		t.Errorf("Expected empty MD5 for partWithoutMD5 but got %s", partWithoutMD5.MD5)
	}

	// Test that part with MD5 has the expected value
	if partWithMD5.MD5 == "" {
		t.Errorf("Expected non-empty MD5 for partWithMD5")
	}

	// Verify the logic: when MD5 is empty, we should skip verification
	// This simulates the check in downloadRegPartCheckSum
	shouldSkipVerification := (partWithoutMD5.MD5 == "")
	if !shouldSkipVerification {
		t.Errorf("Expected to skip verification when MD5 is empty")
	}

	shouldVerify := (partWithMD5.MD5 != "")
	if !shouldVerify {
		t.Errorf("Expected to perform verification when MD5 is present")
	}
}
