package dxda

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestChunkedResumeFinalizesCompletedChildren(t *testing.T) {
	const fileID = "file-resume-finalize-pipeline-0001"
	const fileName = "resume-finalize-pipeline.bin"
	const partSize = 12
	const chunkSize = 4

	expected := []byte("abcdefghijkl")
	md5Sum := md5.Sum(expected)
	expectedMD5 := hex.EncodeToString(md5Sum[:])
	var mu sync.Mutex
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		http.Error(w, "unexpected request: completed chunks need no download URL", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	st := newTestState(t)
	configureRecoveryTestAPI(t, st, server.URL)
	st.maxChunkSize = chunkSize
	st.regularPartSplitThreshold = chunkSize
	st.enableRegularPartChunking = true

	reg := DXFileRegular{
		Folder: testFolder,
		Id:     fileID,
		ProjId: "project-resume-finalize-pipeline-0001",
		Name:   fileName,
		Size:   partSize,
		Parts:  []DXPart{{Id: 1, Size: partSize, MD5: expectedMD5}},
	}
	insertRecoveryTestPart(t, st, reg)
	st.PrepareFilesForDownload(Manifest{Files: []DXFile{reg}})
	if err := os.WriteFile("."+testFolder+"/"+fileName, expected, 0666); err != nil {
		t.Fatalf("Could not seed completed local file: %s", err)
	}
	if _, err := st.db.Exec(
		"UPDATE manifest_regular_chunk_stats SET bytes_fetched = size, download_done_time = 1 WHERE file_id = ? AND part_id = 1",
		fileID,
	); err != nil {
		t.Fatalf("Could not persist completed chunks: %s", err)
	}

	st.DownloadManifestDB("manifest.json.bz2")

	mu.Lock()
	requestCount := requests
	mu.Unlock()
	if requestCount != 0 {
		t.Fatalf("Completed child chunks caused unnecessary HTTP/API requests: %d", requestCount)
	}
	got, err := os.ReadFile("." + testFolder + "/" + fileName)
	if err != nil {
		t.Fatalf("Could not read resumed local file: %s", err)
	}
	if !bytes.Equal(got, expected) {
		t.Fatalf("Local file changed during reconciliation: got %q, want %q", got, expected)
	}

	var parentBytes, integrityDone int
	if err := st.db.QueryRow(
		"SELECT bytes_fetched, integrity_done FROM manifest_regular_stats WHERE file_id = ? AND part_id = 1",
		fileID,
	).Scan(&parentBytes, &integrityDone); err != nil {
		t.Fatalf("Could not read reconciled parent: %s", err)
	}
	var childCount, completedChildren int
	if err := st.db.QueryRow(
		"SELECT COUNT(*), COALESCE(SUM(CASE WHEN bytes_fetched = size THEN 1 ELSE 0 END), 0) FROM manifest_regular_chunk_stats WHERE file_id = ? AND part_id = 1",
		fileID,
	).Scan(&childCount, &completedChildren); err != nil {
		t.Fatalf("Could not read reconciled children: %s", err)
	}
	if parentBytes != partSize || integrityDone != 1 || childCount != 3 || completedChildren != 3 {
		t.Fatalf("Unexpected reconciled SQLite state: parent=%d/%d children=%d/%d", parentBytes, integrityDone, completedChildren, childCount)
	}
	t.Logf("F1 reconciled parent bytes=%d integrity_done=%d; children=%d/%d; HTTP requests=%d",
		parentBytes, integrityDone, completedChildren, childCount, requestCount)
}

const f2ChildEnv = "DXDA_F2_LATEST_TEST_CHILD"

func TestChunkedChecksumFailureTerminates(t *testing.T) {
	if os.Getenv(f2ChildEnv) == "1" {
		testChunkedChecksumFailureTerminatesChild(t)
		return
	}

	const fileID = "file-finalize-budget-pipeline-0001"
	var mu sync.Mutex
	apiCalls := 0
	var ranges []string
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/"+fileID+"/download" {
			mu.Lock()
			apiCalls++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(DXDownloadURL{URL: "http://" + r.Host + "/data", Headers: map[string]string{}})
			return
		}
		if r.Method != http.MethodGet || r.URL.Path != "/data" {
			http.NotFound(w, r)
			return
		}

		rangeValue := r.Header.Get("Range")
		var start, end int
		if _, err := fmt.Sscanf(rangeValue, "bytes=%d-%d", &start, &end); err != nil || start < 0 || end < start || end >= 12 || end-start+1 > 4 {
			http.Error(w, "invalid Range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		mu.Lock()
		ranges = append(ranges, rangeValue)
		mu.Unlock()
		body := bytes.Repeat([]byte("X"), end-start+1)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/12", start, end))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)

	workDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestChunkedChecksumFailureTerminates$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), f2ChildEnv+"=1", "F2_SERVER_URL="+server.URL, "TMPDIR="+workDir)
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("F2 child did not terminate within the bounded timeout: %s\n%s", ctx.Err(), output)
	}
	if err != nil {
		t.Fatalf("F2 child did not complete its expected-error assertion: %s\n%s", err, output)
	}
	if !bytes.Contains(output, []byte("F2_TERMINAL_ERROR=")) {
		t.Fatalf("F2 child did not report the expected terminal error:\n%s", output)
	}

	mu.Lock()
	gotRanges := append([]string(nil), ranges...)
	observedAPICalls := apiCalls
	mu.Unlock()
	sort.Strings(gotRanges)
	wantRanges := []string{
		"bytes=0-3", "bytes=0-3",
		"bytes=4-7", "bytes=4-7",
		"bytes=8-11", "bytes=8-11",
	}
	if strings.Join(gotRanges, ",") != strings.Join(wantRanges, ",") {
		t.Fatalf("Expected two complete recovery rounds of ranges, got %v", gotRanges)
	}
	if observedAPICalls != 2 {
		t.Fatalf("Expected one download URL acquisition per recovery round (2 total), got %d", observedAPICalls)
	}
	t.Logf("F2 terminal failure after bounded retries; ranges=%v API calls=%d", gotRanges, observedAPICalls)
	t.Logf("F2 child output:\n%s", output)
}

func testChunkedChecksumFailureTerminatesChild(t *testing.T) {
	serverURL := os.Getenv("F2_SERVER_URL")
	if serverURL == "" {
		t.Fatal("F2_SERVER_URL is not set")
	}
	parsedServerURL, err := url.Parse(serverURL)
	if err != nil {
		t.Fatalf("Could not parse F2 server URL: %s", err)
	}
	host, portText, err := net.SplitHostPort(parsedServerURL.Host)
	if err != nil {
		t.Fatalf("Could not split F2 server address: %s", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("Could not parse F2 server port: %s", err)
	}

	st := newTestState(t)
	st.dxEnv.ApiServerHost = host
	st.dxEnv.ApiServerPort = port
	st.dxEnv.ApiServerProtocol = parsedServerURL.Scheme
	st.opts.NumThreads = 3
	st.maxChunkSize = 4
	st.regularPartSplitThreshold = 4
	st.enableRegularPartChunking = true
	st.maxInflightChunksPerFile = 2

	content := []byte("abcdefghijkl")
	md5Sum := md5.Sum(content)
	reg := DXFileRegular{
		Folder: testFolder,
		Id:     "file-finalize-budget-pipeline-0001",
		ProjId: "project-finalize-budget-pipeline-0001",
		Name:   "finalize-budget-pipeline.bin",
		Size:   int64(len(content)),
		Parts:  []DXPart{{Id: 1, Size: len(content), MD5: hex.EncodeToString(md5Sum[:])}},
	}
	txn, err := st.db.Begin()
	if err != nil {
		t.Fatalf("Could not begin manifest transaction: %s", err)
	}
	st.addRegularFileToTable(txn, reg)
	if err := txn.Commit(); err != nil {
		t.Fatalf("Could not commit manifest part and chunk rows: %s", err)
	}
	st.PrepareFilesForDownload(Manifest{Files: []DXFile{reg}})

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		st.DownloadManifestDB("manifest.json.bz2")
	}()
	if recovered == nil {
		t.Fatal("Expected a terminal checksum-finalization error, but DownloadManifestDB returned normally")
	}
	if !strings.Contains(fmt.Sprint(recovered), "chunked regular part finalize failed after 2 attempts") {
		t.Fatalf("Unexpected DownloadManifestDB panic: %v", recovered)
	}

	var parentBytes, integrityDone int
	if err := st.db.QueryRow(
		"SELECT bytes_fetched, integrity_done FROM manifest_regular_stats WHERE file_id = ? AND part_id = 1",
		reg.Id,
	).Scan(&parentBytes, &integrityDone); err != nil {
		t.Fatalf("Could not read terminal parent state: %s", err)
	}
	var childCount, completedChildren, childBytes int
	if err := st.db.QueryRow(
		"SELECT COUNT(*), COALESCE(SUM(CASE WHEN bytes_fetched = size THEN 1 ELSE 0 END), 0), COALESCE(SUM(bytes_fetched), 0) FROM manifest_regular_chunk_stats WHERE file_id = ? AND part_id = 1",
		reg.Id,
	).Scan(&childCount, &completedChildren, &childBytes); err != nil {
		t.Fatalf("Could not read terminal child state: %s", err)
	}
	if parentBytes != 0 || integrityDone != 0 || childCount != 3 || completedChildren != 3 || childBytes != len(content) {
		t.Fatalf("Unexpected terminal SQLite state: parent=%d/%d children=%d/%d child_bytes=%d/%d",
			parentBytes, integrityDone, completedChildren, childCount, childBytes, len(content))
	}
	t.Logf("F2_TERMINAL_ERROR=%v; parent bytes=%d integrity_done=%d; children=%d/%d child_bytes=%d/%d",
		recovered, parentBytes, integrityDone, completedChildren, childCount, childBytes, len(content))
}

func configureRecoveryTestAPI(t *testing.T, st *State, serverURL string) {
	t.Helper()
	parsedServerURL, err := url.Parse(serverURL)
	if err != nil {
		t.Fatalf("Could not parse test server URL: %s", err)
	}
	host, portText, err := net.SplitHostPort(parsedServerURL.Host)
	if err != nil {
		t.Fatalf("Could not split test server address: %s", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("Could not parse test server port: %s", err)
	}
	st.dxEnv.ApiServerHost = host
	st.dxEnv.ApiServerPort = port
	st.dxEnv.ApiServerProtocol = parsedServerURL.Scheme
}

func insertRecoveryTestPart(t *testing.T, st *State, reg DXFileRegular) {
	t.Helper()
	txn, err := st.db.Begin()
	if err != nil {
		t.Fatalf("Could not begin manifest transaction: %s", err)
	}
	st.addRegularFileToTable(txn, reg)
	if err := txn.Commit(); err != nil {
		t.Fatalf("Could not commit manifest part and chunk rows: %s", err)
	}
}
