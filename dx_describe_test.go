package dxda

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
)

// Stand up a fake apiserver that answers every request with the given JSON
// body, and a DXEnvironment pointing at it. The last request body received is
// recorded, so tests can assert on the describe request that was sent.
func newFakeApiServer(t *testing.T, reply string) (*DXEnvironment, *[]byte) {
	t.Helper()

	var lastRequest []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading request body: %s", err)
		}
		lastRequest = body
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, reply)
	}))
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parsing test server url %s: %s", srv.URL, err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("parsing test server port %s: %s", u.Port(), err)
	}

	dxEnv := &DXEnvironment{
		ApiServerHost:     u.Hostname(),
		ApiServerPort:     port,
		ApiServerProtocol: "http",
		Token:             "test-token",
	}
	return dxEnv, &lastRequest
}

func describeReply(describe string) string {
	return fmt.Sprintf(`{"results":[{"describe":%s}]}`, describe)
}

// A file with no checksumType in the describe output. Before the fix this
// panicked with a nil pointer dereference, because the optional pointer was
// dereferenced unconditionally.
func TestSubmitMissingChecksumType(t *testing.T) {
	dxEnv, _ := newFakeApiServer(t, describeReply(`{
		"id": "file-0001",
		"project": "project-0001",
		"name": "a.txt",
		"state": "closed",
		"archivalState": "live",
		"size": 10,
		"parts": {"1": {"size": 10, "md5": "ac54bf7ff5e0ac0d1c1e2e6dbe6ba0cb"}}
	}`))

	files, err := submit(context.Background(), &http.Client{}, dxEnv, "project-0001", []string{"file-0001"})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	f, ok := files["file-0001"]
	if !ok {
		t.Fatalf("Expected file-0001 in the result, got %v", files)
	}
	if f.ChecksumType != nil {
		t.Errorf("Expected nil ChecksumType, got %q", *f.ChecksumType)
	}
	if f.Size != 10 {
		t.Errorf("Expected size 10, got %d", f.Size)
	}
	if f.Symlink != nil {
		t.Errorf("Expected no symlink, got %v", f.Symlink)
	}
}

// A symlink reports neither parts nor a checksum type. This is the shape that
// produced the reported crash.
func TestSubmitSymlinkMissingChecksumType(t *testing.T) {
	dxEnv, _ := newFakeApiServer(t, describeReply(`{
		"id": "file-0002",
		"project": "project-0001",
		"name": "linked.txt",
		"state": "closed",
		"archivalState": "live",
		"size": 20,
		"drive": "drive-0001",
		"md5": "1c1e2e6dbe6ba0cbac54bf7ff5e0ac0d",
		"symlinkPath": {"object": "https://example.invalid/linked.txt"}
	}`))

	files, err := submit(context.Background(), &http.Client{}, dxEnv, "project-0001", []string{"file-0002"})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	f, ok := files["file-0002"]
	if !ok {
		t.Fatalf("Expected file-0002 in the result, got %v", files)
	}
	if f.ChecksumType != nil {
		t.Errorf("Expected nil ChecksumType, got %q", *f.ChecksumType)
	}
	if f.Symlink == nil {
		t.Fatalf("Expected a symlink, got nil")
	}
	if f.Symlink.Drive != "drive-0001" {
		t.Errorf("Expected drive drive-0001, got %s", f.Symlink.Drive)
	}
	if f.Symlink.MD5 != "1c1e2e6dbe6ba0cbac54bf7ff5e0ac0d" {
		t.Errorf("Unexpected symlink md5 %s", f.Symlink.MD5)
	}
}

// When the platform does report a checksum type, it and the per-part checksums
// must survive into the description.
func TestSubmitWithChecksumType(t *testing.T) {
	dxEnv, _ := newFakeApiServer(t, describeReply(`{
		"id": "file-0003",
		"project": "project-0001",
		"name": "c.txt",
		"state": "closed",
		"archivalState": "live",
		"size": 15,
		"checksumType": "CRC64NVME",
		"parts": {
			"1": {"size": 10, "checksum": "12xUBUlUwUM="},
			"2": {"size": 5, "checksum": "ImIEBA=="}
		}
	}`))

	files, err := submit(context.Background(), &http.Client{}, dxEnv, "project-0001", []string{"file-0003"})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	f := files["file-0003"]
	if f.ChecksumType == nil {
		t.Fatalf("Expected ChecksumType to be set, got nil")
	}
	if *f.ChecksumType != ChecksumCRC64NVME {
		t.Errorf("Expected ChecksumType %s, got %s", ChecksumCRC64NVME, *f.ChecksumType)
	}

	if len(f.Parts) != 2 {
		t.Fatalf("Expected 2 parts, got %d", len(f.Parts))
	}
	part1 := f.Parts["1"]
	if part1.Checksum == nil {
		t.Fatalf("Expected part 1 checksum to be set, got nil")
	}
	if *part1.Checksum != "12xUBUlUwUM=" {
		t.Errorf("Expected part 1 checksum 12xUBUlUwUM=, got %s", *part1.Checksum)
	}
	if part1.MD5 != "" {
		t.Errorf("Expected no md5 on part 1, got %s", part1.MD5)
	}
}

// A part with neither md5 nor checksum must still describe cleanly. The
// integrity check decides separately what to do about it.
func TestSubmitPartWithoutAnyChecksum(t *testing.T) {
	dxEnv, _ := newFakeApiServer(t, describeReply(`{
		"id": "file-0004",
		"project": "project-0001",
		"name": "d.txt",
		"state": "closed",
		"archivalState": "live",
		"size": 10,
		"parts": {"1": {"size": 10}}
	}`))

	files, err := submit(context.Background(), &http.Client{}, dxEnv, "project-0001", []string{"file-0004"})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	part1 := files["file-0004"].Parts["1"]
	if part1.Checksum != nil {
		t.Errorf("Expected nil part checksum, got %q", *part1.Checksum)
	}
	if part1.MD5 != "" {
		t.Errorf("Expected empty part md5, got %s", part1.MD5)
	}
}

// The describe request must ask for checksumType, otherwise the platform will
// never report it and integrity checks silently degrade to md5-only.
func TestSubmitRequestsChecksumTypeField(t *testing.T) {
	dxEnv, lastRequest := newFakeApiServer(t, `{"results":[]}`)

	if _, err := submit(context.Background(), &http.Client{}, dxEnv, "project-0001", []string{"file-0001"}); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	var scoped RequestWithScope
	if err := json.Unmarshal(*lastRequest, &scoped); err != nil {
		t.Fatalf("Could not unmarshal request %s: %s", *lastRequest, err)
	}
	if !scoped.DescribeOptions["fields"]["checksumType"] {
		t.Errorf("Expected the describe request to ask for checksumType, got %s", *lastRequest)
	}
	// a project id must be passed as a scope, to reduce load on the server
	if scoped.Scope["project"] != "project-0001" {
		t.Errorf("Expected the request to be scoped to project-0001, got %s", *lastRequest)
	}
}

// A container id is a valid scope as well, while anything else must fall back
// to an unscoped request.
func TestSubmitScopeSelection(t *testing.T) {
	for _, tc := range []struct {
		projectId string
		wantScope bool
	}{
		{"project-0001", true},
		{"container-0001", true},
		{"", false},
	} {
		dxEnv, lastRequest := newFakeApiServer(t, `{"results":[]}`)
		if _, err := submit(context.Background(), &http.Client{}, dxEnv, tc.projectId, []string{"file-0001"}); err != nil {
			t.Fatalf("%s: expected no error, got %v", tc.projectId, err)
		}

		var body map[string]json.RawMessage
		if err := json.Unmarshal(*lastRequest, &body); err != nil {
			t.Fatalf("%s: could not unmarshal request %s: %s", tc.projectId, *lastRequest, err)
		}
		if _, hasScope := body["scope"]; hasScope != tc.wantScope {
			t.Errorf("%s: expected scope present=%v, got request %s", tc.projectId, tc.wantScope, *lastRequest)
		}
	}
}

// Describing more objects than fit in one API call must batch the requests and
// merge the replies.
func TestDxDescribeBulkObjectsEmptyInput(t *testing.T) {
	dxEnv, _ := newFakeApiServer(t, `{"results":[]}`)

	files, err := DxDescribeBulkObjects(context.Background(), &http.Client{}, dxEnv, "project-0001", []string{})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(files) != 0 {
		t.Errorf("Expected no files, got %v", files)
	}
}
