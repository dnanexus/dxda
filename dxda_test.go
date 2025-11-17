package dxda_test

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

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

func TestdownloadRegPartCheckSum_WithValidMD5(t *testing.T) {
	// Create test data
	testData := []byte("Hello, World!")
	expectedMD5 := md5.Sum(testData)
	expectedMD5Str := hex.EncodeToString(expectedMD5[:])

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(testData)
	}))
	defer server.Close()

	// Create temporary file
	tempFile, err := os.CreateTemp("", "test_download_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	// Create test directory structure
	testDir := ".test_folder"
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	testFileName := "test_file.txt"
	testFilePath := fmt.Sprintf("%s/%s", testDir, testFileName)

	// Create the test file
	testFile, err := os.Create(testFilePath)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	testFile.Close()

	// Setup test state
	dxEnv := dxda.DXEnvironment{}
	opts := dxda.Opts{Verbose: true}
	st := dxda.NewDxDa(dxEnv, "test_manifest", opts)
	defer st.Close()

	// Create DBPartRegular with valid MD5
	part := dxda.DBPartRegular{
		FileId:           "test-file-123",
		Project:          "test-project",
		FileName:         testFileName,
		Folder:           "test_folder",
		PartId:           1,
		Offset:           0,
		Size:             len(testData),
		MD5:              expectedMD5Str,
		BytesFetched:     0,
		DownloadDoneTime: 0,
	}

	// Create download URL
	dxURL := dxda.DXDownloadURL{
		URL:     server.URL,
		Headers: map[string]string{},
	}

	// Create HTTP client and memory buffer
	httpClient := &http.Client{Timeout: 30 * time.Second}
	memoryBuf := make([]byte, 1024)

	// Test successful download with valid MD5
	success, err := st.downloadRegPartCheckSum(httpClient, part, dxURL, memoryBuf)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if !success {
		t.Errorf("Expected download to succeed with valid MD5")
	}
}

func TestdownloadRegPartCheckSum_WithEmptyMD5(t *testing.T) {
	// Create test data
	testData := []byte("Hello, World! This should pass even without MD5.")

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(testData)
	}))
	defer server.Close()

	// Create test directory structure
	testDir := ".test_folder_no_md5"
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	testFileName := "test_file_no_md5.txt"
	testFilePath := fmt.Sprintf("%s/%s", testDir, testFileName)

	// Create the test file
	testFile, err := os.Create(testFilePath)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	testFile.Close()

	// Setup test state
	dxEnv := dxda.DXEnvironment{}
	opts := dxda.Opts{Verbose: true}
	st := dxda.NewDxDa(dxEnv, "test_manifest_no_md5", opts)
	defer st.Close()

	// Create DBPartRegular with empty MD5
	part := dxda.DBPartRegular{
		FileId:           "test-file-456",
		Project:          "test-project",
		FileName:         testFileName,
		Folder:           "test_folder_no_md5",
		PartId:           1,
		Offset:           0,
		Size:             len(testData),
		MD5:              "", // Empty MD5 - should skip validation
		BytesFetched:     0,
		DownloadDoneTime: 0,
	}

	// Create download URL
	dxURL := dxda.DXDownloadURL{
		URL:     server.URL,
		Headers: map[string]string{},
	}

	// Create HTTP client and memory buffer
	httpClient := &http.Client{Timeout: 30 * time.Second}
	memoryBuf := make([]byte, 1024)

	// Test successful download with empty MD5 (should skip validation)
	success, err := st.downloadRegPartCheckSum(httpClient, part, dxURL, memoryBuf)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if !success {
		t.Errorf("Expected download to succeed even without MD5 checksum")
	}
}

func TestdownloadRegPartCheckSum_WithInvalidMD5(t *testing.T) {
	// Create test data
	testData := []byte("Hello, World!")
	invalidMD5 := "this_is_definitely_not_the_correct_md5"

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(testData)
	}))
	defer server.Close()

	// Create test directory structure
	testDir := ".test_folder_invalid_md5"
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	testFileName := "test_file_invalid_md5.txt"
	testFilePath := fmt.Sprintf("%s/%s", testDir, testFileName)

	// Create the test file
	testFile, err := os.Create(testFilePath)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	testFile.Close()

	// Setup test state
	dxEnv := dxda.DXEnvironment{}
	opts := dxda.Opts{Verbose: false} // Turn off verbose to avoid log spam
	st := dxda.NewDxDa(dxEnv, "test_manifest_invalid_md5", opts)
	defer st.Close()

	// Create DBPartRegular with invalid MD5
	part := dxda.DBPartRegular{
		FileId:           "test-file-789",
		Project:          "test-project",
		FileName:         testFileName,
		Folder:           "test_folder_invalid_md5",
		PartId:           1,
		Offset:           0,
		Size:             len(testData),
		MD5:              invalidMD5, // Invalid MD5 - should fail validation
		BytesFetched:     0,
		DownloadDoneTime: 0,
	}

	// Create download URL
	dxURL := dxda.DXDownloadURL{
		URL:     server.URL,
		Headers: map[string]string{},
	}

	// Create HTTP client and memory buffer
	httpClient := &http.Client{Timeout: 30 * time.Second}
	memoryBuf := make([]byte, 1024)

	// Test failed download with invalid MD5
	success, err := st.downloadRegPartCheckSum(httpClient, part, dxURL, memoryBuf)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if success {
		t.Errorf("Expected download to fail with invalid MD5 checksum")
	}
}
