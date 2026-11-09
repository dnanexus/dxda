package dxda

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestChunkedRegularPartParallelDownload(t *testing.T) {
	const fileID = "file-parallel-download-0001"
	const fileName = "parallel-download.bin"
	const partSize = 16
	const chunkSize = 4
	const workerCount = 4
	const perFileCap = 2

	expected := []byte("0123456789abcdef")
	md5Sum := md5.Sum(expected)
	expectedMD5 := hex.EncodeToString(md5Sum[:])

	var server *httptest.Server
	var mu sync.Mutex
	active, peak, apiCalls := 0, 0, 0
	barrierTimedOut := false
	var ranges []string
	gate := make(chan struct{})
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(gate) }) }
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/"+fileID+"/download" {
			mu.Lock()
			apiCalls++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(DXDownloadURL{URL: server.URL + "/data", Headers: map[string]string{}})
			return
		}
		if r.Method != http.MethodGet || r.URL.Path != "/data" {
			http.NotFound(w, r)
			return
		}

		rangeValue := r.Header.Get("Range")
		mu.Lock()
		active++
		if active > peak {
			peak = active
		}
		ranges = append(ranges, rangeValue)
		if active >= 2 {
			releaseGate()
		}
		mu.Unlock()
		defer func() {
			mu.Lock()
			active--
			mu.Unlock()
		}()

		barrierTimer := time.NewTimer(5 * time.Second)
		defer barrierTimer.Stop()
		select {
		case <-gate:
		case <-barrierTimer.C:
			mu.Lock()
			barrierTimedOut = true
			mu.Unlock()
			releaseGate()
		}

		var start, end int
		if _, err := fmt.Sscanf(rangeValue, "bytes=%d-%d", &start, &end); err != nil ||
			start < 0 || end < start || end >= partSize || end-start+1 > chunkSize {
			http.Error(w, "invalid Range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		body := expected[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, partSize))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	}))
	t.Cleanup(func() {
		releaseGate()
		server.Close()
	})

	parsedServerURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("Could not parse test server URL: %s", err)
	}
	serverHost, serverPortText, err := net.SplitHostPort(parsedServerURL.Host)
	if err != nil {
		t.Fatalf("Could not split test server address: %s", err)
	}
	serverPort, err := strconv.Atoi(serverPortText)
	if err != nil {
		t.Fatalf("Could not parse test server port: %s", err)
	}

	st := newTestState(t)
	st.dxEnv.ApiServerHost = serverHost
	st.dxEnv.ApiServerPort = serverPort
	st.dxEnv.ApiServerProtocol = parsedServerURL.Scheme
	st.opts.NumThreads = workerCount
	st.maxChunkSize = chunkSize
	st.regularPartSplitThreshold = chunkSize
	st.enableRegularPartChunking = true
	st.maxInflightChunksPerFile = perFileCap

	reg := DXFileRegular{
		Folder: testFolder,
		Id:     fileID,
		ProjId: "project-parallel-download-0001",
		Name:   fileName,
		Size:   partSize,
		Parts:  []DXPart{{Id: 1, Size: partSize, MD5: expectedMD5}},
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

	st.DownloadManifestDB("manifest.json.bz2")

	downloaded, err := os.ReadFile("." + testFolder + "/" + fileName)
	if err != nil {
		t.Fatalf("Could not read downloaded file: %s", err)
	}
	if !bytes.Equal(downloaded, expected) {
		t.Fatalf("Downloaded bytes differ from expected content: got %q, want %q", downloaded, expected)
	}
	actualMD5 := md5.Sum(downloaded)
	if got := hex.EncodeToString(actualMD5[:]); got != expectedMD5 {
		t.Fatalf("Fixture MD5 mismatch: got %s, want %s", got, expectedMD5)
	}

	var parentBytes, integrityDone int
	var storedMD5 string
	if err := st.db.QueryRow(
		"SELECT bytes_fetched, integrity_done, md5 FROM manifest_regular_stats WHERE file_id = ? AND part_id = 1",
		fileID,
	).Scan(&parentBytes, &integrityDone, &storedMD5); err != nil {
		t.Fatalf("Could not read parent state: %s", err)
	}
	if parentBytes != partSize || integrityDone != 1 || storedMD5 != expectedMD5 {
		t.Fatalf("Unexpected parent state: bytes_fetched=%d integrity_done=%d md5=%q", parentBytes, integrityDone, storedMD5)
	}
	var childCount, completedChildren, childBytes int
	if err := st.db.QueryRow(
		`SELECT COUNT(*),
			COALESCE(SUM(CASE WHEN bytes_fetched = size THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(bytes_fetched), 0)
		 FROM manifest_regular_chunk_stats WHERE file_id = ? AND part_id = 1`,
		fileID,
	).Scan(&childCount, &completedChildren, &childBytes); err != nil {
		t.Fatalf("Could not read child state: %s", err)
	}
	if childCount != 4 || completedChildren != 4 || childBytes != partSize {
		t.Fatalf("Unexpected child state: rows=%d complete=%d bytes=%d", childCount, completedChildren, childBytes)
	}

	mu.Lock()
	gotRanges := append([]string(nil), ranges...)
	observedPeak, observedAPICalls, gateExpired := peak, apiCalls, barrierTimedOut
	mu.Unlock()
	sort.Strings(gotRanges)
	wantRanges := []string{"bytes=0-3", "bytes=12-15", "bytes=4-7", "bytes=8-11"}
	if strings.Join(gotRanges, ",") != strings.Join(wantRanges, ",") {
		t.Fatalf("Unexpected Range requests: got %v, want %v", gotRanges, wantRanges)
	}
	if observedAPICalls != 1 {
		t.Fatalf("Expected one download URL API call, got %d", observedAPICalls)
	}
	if gateExpired {
		t.Fatal("Two Range handlers did not overlap before the bounded barrier timed out")
	}
	if observedPeak < 2 {
		t.Fatalf("Expected at least two concurrent Range handlers, peak was %d", observedPeak)
	}
	if observedPeak > perFileCap {
		t.Fatalf("Per-file in-flight cap exceeded: peak=%d cap=%d", observedPeak, perFileCap)
	}
	t.Logf("Range requests=%v peak_concurrency=%d per_file_cap=%d", gotRanges, observedPeak, perFileCap)
	t.Logf("SQLite parent bytes=%d integrity_done=%d md5=%s; children=%d/%d bytes=%d/%d",
		parentBytes, integrityDone, storedMD5, completedChildren, childCount, childBytes, partSize)
}

func TestChunkedRegularPartPartialResume(t *testing.T) {
	const fileID = "file-partial-resume-0001"
	const fileName = "partial-resume.bin"
	const partSize = 16
	const chunkSize = 4

	expected := []byte("abcdefghijklmnop")
	md5Sum := md5.Sum(expected)
	expectedMD5 := hex.EncodeToString(md5Sum[:])
	var server *httptest.Server
	var mu sync.Mutex
	var ranges []string
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/"+fileID+"/download" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(DXDownloadURL{URL: server.URL + "/data", Headers: map[string]string{}})
			return
		}
		if r.Method != http.MethodGet || r.URL.Path != "/data" {
			http.NotFound(w, r)
			return
		}

		rangeValue := r.Header.Get("Range")
		var start, end int
		if _, err := fmt.Sscanf(rangeValue, "bytes=%d-%d", &start, &end); err != nil ||
			start < chunkSize || end < start || end >= partSize || end-start+1 > chunkSize {
			http.Error(w, "unexpected Range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		mu.Lock()
		ranges = append(ranges, rangeValue)
		mu.Unlock()
		body := expected[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, partSize))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)

	parsedServerURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("Could not parse test server URL: %s", err)
	}
	serverHost, serverPortText, err := net.SplitHostPort(parsedServerURL.Host)
	if err != nil {
		t.Fatalf("Could not split test server address: %s", err)
	}
	serverPort, err := strconv.Atoi(serverPortText)
	if err != nil {
		t.Fatalf("Could not parse test server port: %s", err)
	}

	st := newTestState(t)
	st.dxEnv.ApiServerHost = serverHost
	st.dxEnv.ApiServerPort = serverPort
	st.dxEnv.ApiServerProtocol = parsedServerURL.Scheme
	st.opts.NumThreads = 4
	st.maxChunkSize = chunkSize
	st.regularPartSplitThreshold = chunkSize
	st.enableRegularPartChunking = true
	st.maxInflightChunksPerFile = 2

	reg := DXFileRegular{
		Folder: testFolder,
		Id:     fileID,
		ProjId: "project-partial-resume-0001",
		Name:   fileName,
		Size:   partSize,
		Parts:  []DXPart{{Id: 1, Size: partSize, MD5: expectedMD5}},
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

	localFile, err := os.OpenFile("."+testFolder+"/"+fileName, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Could not open output file to seed completed chunk: %s", err)
	}
	if _, err := localFile.WriteAt(expected[:chunkSize], 0); err != nil {
		localFile.Close()
		t.Fatalf("Could not seed completed chunk bytes: %s", err)
	}
	if err := localFile.Close(); err != nil {
		t.Fatalf("Could not close seeded output file: %s", err)
	}
	result, err := st.db.Exec(
		"UPDATE manifest_regular_chunk_stats SET bytes_fetched = size, download_done_time = 1 WHERE file_id = ? AND part_id = 1 AND chunk_id = 1",
		fileID,
	)
	if err != nil {
		t.Fatalf("Could not mark first child chunk complete: %s", err)
	}
	if rowsAffected, err := result.RowsAffected(); err != nil || rowsAffected != 1 {
		t.Fatalf("Expected to seed exactly one completed chunk, rows=%d err=%v", rowsAffected, err)
	}

	st.DownloadManifestDB("manifest.json.bz2")

	downloaded, err := os.ReadFile("." + testFolder + "/" + fileName)
	if err != nil {
		t.Fatalf("Could not read downloaded file: %s", err)
	}
	if !bytes.Equal(downloaded, expected) {
		t.Fatalf("Resumed output differs from expected content: got %q, want %q", downloaded, expected)
	}
	actualMD5 := md5.Sum(downloaded)
	if got := hex.EncodeToString(actualMD5[:]); got != expectedMD5 {
		t.Fatalf("Resumed output MD5 mismatch: got %s, want %s", got, expectedMD5)
	}

	mu.Lock()
	gotRanges := append([]string(nil), ranges...)
	mu.Unlock()
	sort.Strings(gotRanges)
	wantRanges := []string{"bytes=12-15", "bytes=4-7", "bytes=8-11"}
	if strings.Join(gotRanges, ",") != strings.Join(wantRanges, ",") {
		t.Fatalf("Resume requested wrong ranges: got %v, want %v", gotRanges, wantRanges)
	}

	var parentBytes, integrityDone int
	var storedMD5 string
	if err := st.db.QueryRow(
		"SELECT bytes_fetched, integrity_done, md5 FROM manifest_regular_stats WHERE file_id = ? AND part_id = 1",
		fileID,
	).Scan(&parentBytes, &integrityDone, &storedMD5); err != nil {
		t.Fatalf("Could not read resumed parent state: %s", err)
	}
	if parentBytes != partSize || integrityDone != 1 || storedMD5 != expectedMD5 {
		t.Fatalf("Unexpected resumed parent state: bytes_fetched=%d integrity_done=%d md5=%q", parentBytes, integrityDone, storedMD5)
	}
	var childCount, completedChildren, childBytes int
	if err := st.db.QueryRow(
		`SELECT COUNT(*),
			COALESCE(SUM(CASE WHEN bytes_fetched = size THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(bytes_fetched), 0)
		 FROM manifest_regular_chunk_stats WHERE file_id = ? AND part_id = 1`,
		fileID,
	).Scan(&childCount, &completedChildren, &childBytes); err != nil {
		t.Fatalf("Could not read resumed child state: %s", err)
	}
	if childCount != 4 || completedChildren != 4 || childBytes != partSize {
		t.Fatalf("Unexpected resumed child state: rows=%d complete=%d bytes=%d", childCount, completedChildren, childBytes)
	}
	t.Logf("Resume Range requests=%v; SQLite parent bytes=%d integrity_done=%d; children=%d/%d bytes=%d/%d",
		gotRanges, parentBytes, integrityDone, completedChildren, childCount, childBytes, partSize)
}
