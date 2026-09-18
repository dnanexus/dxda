package dxda

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// The classic test vector, so that the checksums below are the well-known
// published values rather than something this package computed for itself.
const (
	testPartContent = "The quick brown fox jumps over the lazy dog"
	testPartMD5     = "9e107d9d372bb6826bd81d3542a419d6"
	testPartCRC64   = "12xUBUlUwUM="
	testPartSHA1    = "2fd4e1c67a2d28fced849ee1bb76e7391b93eb12"
)

const testFolder = "/sub"

// Set up a State backed by a real sqlite database in a temporary working
// directory. The integrity checker resolves part files relative to the working
// directory, so the tests have to run inside it.
func newTestState(t *testing.T) *State {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Could not determine the working directory: %s", err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatalf("Could not change to the temporary directory: %s", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(wd); err != nil {
			t.Errorf("Could not restore the working directory: %s", err)
		}
	})

	st := NewDxDa(DXEnvironment{Token: "test-token"}, "manifest.json.bz2", Opts{NumThreads: 1})
	t.Cleanup(st.Close)

	// creates the schema, without inserting any rows
	st.CreateManifestDB(Manifest{Files: []DXFile{}}, "manifest.json.bz2")
	return st
}

// Write the on-disk file that holds the given parts, back to back.
func writePartFile(t *testing.T, name string, parts ...[]byte) {
	t.Helper()
	if err := os.MkdirAll("."+testFolder, 0777); err != nil {
		t.Fatalf("Could not create the part folder: %s", err)
	}
	if err := os.WriteFile("."+testFolder+"/"+name, bytes.Join(parts, nil), 0666); err != nil {
		t.Fatalf("Could not write the part file: %s", err)
	}
}

// Record a fully downloaded part in the database, so that resetting it has
// something to act on.
func insertPart(t *testing.T, st *State, p DBPartRegular) {
	t.Helper()
	_, err := st.db.Exec(
		`INSERT INTO manifest_regular_stats VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		p.FileId, p.Project, p.FileName, p.Folder, p.PartId, p.Offset, p.Size, p.MD5,
		p.Size, 1, p.ChecksumType, p.Checksum, splitModePartRow, 1)
	if err != nil {
		t.Fatalf("Could not insert the part: %s", err)
	}
}

func checkPart(t *testing.T, st *State, p DBPartRegular) []string {
	t.Helper()
	integrityMsgs := make(chan string, 16)
	st.checkDBPartRegular(p, integrityMsgs)
	close(integrityMsgs)

	var msgs []string
	for msg := range integrityMsgs {
		msgs = append(msgs, msg)
	}
	return msgs
}

func bytesFetched(t *testing.T, st *State, p DBPartRegular) int {
	t.Helper()
	var fetched int
	err := st.db.QueryRow(
		"SELECT bytes_fetched FROM manifest_regular_stats WHERE file_id = ? AND part_id = ?",
		p.FileId, p.PartId).Scan(&fetched)
	if err != nil {
		t.Fatalf("Could not read bytes_fetched: %s", err)
	}
	return fetched
}

// A part covering the whole of a one-part file.
func wholeFilePart(name string, size int) DBPartRegular {
	return DBPartRegular{
		FileId:   "file-0001",
		Project:  "project-0001",
		FileName: name,
		Folder:   testFolder,
		PartId:   1,
		Offset:   0,
		Size:     size,
	}
}

func expectNoMessages(t *testing.T, msgs []string) {
	t.Helper()
	if len(msgs) != 0 {
		t.Errorf("Expected no integrity messages, got %v", msgs)
	}
}

func expectOneMessageContaining(t *testing.T, msgs []string, want string) {
	t.Helper()
	if len(msgs) != 1 {
		t.Fatalf("Expected exactly one integrity message, got %v", msgs)
	}
	if !strings.Contains(msgs[0], want) {
		t.Errorf("Expected the message to contain %q, got %q", want, msgs[0])
	}
}

func TestCheckPartMD5Match(t *testing.T) {
	st := newTestState(t)

	p := wholeFilePart("a.txt", len(testPartContent))
	p.MD5 = testPartMD5
	writePartFile(t, p.FileName, []byte(testPartContent))
	insertPart(t, st, p)

	expectNoMessages(t, checkPart(t, st, p))
	if got := bytesFetched(t, st, p); got != p.Size {
		t.Errorf("Expected the part to stay downloaded, bytes_fetched = %d", got)
	}
}

func TestCheckPartMD5Mismatch(t *testing.T) {
	st := newTestState(t)

	p := wholeFilePart("a.txt", len(testPartContent))
	p.MD5 = testPartMD5
	writePartFile(t, p.FileName, []byte("The quick brown fox jumps over the lazy cat"))
	insertPart(t, st, p)

	expectOneMessageContaining(t, checkPart(t, st, p), "md5sum mismatch")
	if got := bytesFetched(t, st, p); got != 0 {
		t.Errorf("Expected the part to be reset, bytes_fetched = %d", got)
	}
}

// Every supported checksum type must verify a correct part without complaint.
func TestCheckPartChecksumMatch(t *testing.T) {
	for _, checksumType := range []string{
		ChecksumCRC64NVME, ChecksumCRC32C, ChecksumCRC32, ChecksumSHA256, ChecksumSHA1,
	} {
		st := newTestState(t)

		checksum, err := CalculateChecksum(checksumType, []byte(testPartContent))
		if err != nil {
			t.Fatalf("%s: expected no error, got %v", checksumType, err)
		}

		p := wholeFilePart("a.txt", len(testPartContent))
		p.ChecksumType = checksumType
		p.Checksum = checksum
		writePartFile(t, p.FileName, []byte(testPartContent))
		insertPart(t, st, p)

		if msgs := checkPart(t, st, p); len(msgs) != 0 {
			t.Errorf("%s: expected no integrity messages, got %v", checksumType, msgs)
		}
		if got := bytesFetched(t, st, p); got != p.Size {
			t.Errorf("%s: expected the part to stay downloaded, bytes_fetched = %d", checksumType, got)
		}
	}
}

// Pin the checksum path to published test vectors, so that a change in the
// digest encoding cannot pass unnoticed.
func TestCheckPartChecksumKnownVectors(t *testing.T) {
	for _, tc := range []struct{ checksumType, checksum string }{
		{ChecksumCRC64NVME, testPartCRC64},
		{ChecksumSHA1, testPartSHA1},
	} {
		st := newTestState(t)

		p := wholeFilePart("a.txt", len(testPartContent))
		p.ChecksumType = tc.checksumType
		p.Checksum = tc.checksum
		writePartFile(t, p.FileName, []byte(testPartContent))
		insertPart(t, st, p)

		if msgs := checkPart(t, st, p); len(msgs) != 0 {
			t.Errorf("%s: expected no integrity messages, got %v", tc.checksumType, msgs)
		}
	}
}

func TestCheckPartChecksumMismatch(t *testing.T) {
	st := newTestState(t)

	p := wholeFilePart("a.txt", len(testPartContent))
	p.ChecksumType = ChecksumCRC64NVME
	p.Checksum = testPartCRC64
	writePartFile(t, p.FileName, []byte("The quick brown fox jumps over the lazy cat"))
	insertPart(t, st, p)

	msgs := checkPart(t, st, p)
	expectOneMessageContaining(t, msgs, "checksum mismatch")
	// the message must name the algorithm, so the log says what actually failed
	if !strings.Contains(msgs[0], ChecksumCRC64NVME) {
		t.Errorf("Expected the message to name %s, got %q", ChecksumCRC64NVME, msgs[0])
	}
	if got := bytesFetched(t, st, p); got != 0 {
		t.Errorf("Expected the part to be reset, bytes_fetched = %d", got)
	}
}

// A part with nothing to verify against is not an integrity failure. It used to
// be reported as one, which made `inspect` fail for such files.
func TestCheckPartNoChecksumAvailable(t *testing.T) {
	st := newTestState(t)

	p := wholeFilePart("a.txt", len(testPartContent))
	writePartFile(t, p.FileName, []byte(testPartContent))
	insertPart(t, st, p)

	expectNoMessages(t, checkPart(t, st, p))
	if got := bytesFetched(t, st, p); got != p.Size {
		t.Errorf("Expected the part to be left alone, bytes_fetched = %d", got)
	}
}

// A checksum type with no stored value cannot be checked, and must not be
// compared against the empty string.
func TestCheckPartChecksumTypeWithoutValue(t *testing.T) {
	st := newTestState(t)

	p := wholeFilePart("a.txt", len(testPartContent))
	p.ChecksumType = ChecksumCRC64NVME
	p.Checksum = ""
	writePartFile(t, p.FileName, []byte(testPartContent))
	insertPart(t, st, p)

	expectNoMessages(t, checkPart(t, st, p))
	if got := bytesFetched(t, st, p); got != p.Size {
		t.Errorf("Expected the part to be left alone, bytes_fetched = %d", got)
	}
}

// An algorithm this build cannot compute is reported, but the part is not
// discarded: the data may well be fine.
func TestCheckPartUnsupportedChecksumType(t *testing.T) {
	st := newTestState(t)

	p := wholeFilePart("a.txt", len(testPartContent))
	p.ChecksumType = "SHA512"
	p.Checksum = "whatever"
	writePartFile(t, p.FileName, []byte(testPartContent))
	insertPart(t, st, p)

	expectOneMessageContaining(t, checkPart(t, st, p), "Unsupported checksum type")
	if got := bytesFetched(t, st, p); got != p.Size {
		t.Errorf("Expected the part to be left alone, bytes_fetched = %d", got)
	}
}

func TestCheckPartMissingFile(t *testing.T) {
	st := newTestState(t)

	p := wholeFilePart("a.txt", len(testPartContent))
	p.MD5 = testPartMD5
	insertPart(t, st, p)
	// deliberately no file on disk

	expectOneMessageContaining(t, checkPart(t, st, p), "does not exist")
}

// Each part must be checked over its own byte range only. A part is verified
// with a seek plus a limited read, which is easy to get wrong.
func TestCheckPartVerifiesOnlyItsOwnRange(t *testing.T) {
	st := newTestState(t)

	// part 1 is corrupt, part 2 holds the good content
	corrupt := []byte("XXXXXXXXXX")
	good := []byte(testPartContent)
	writePartFile(t, "a.txt", corrupt, good)

	part2 := DBPartRegular{
		FileId:       "file-0001",
		Project:      "project-0001",
		FileName:     "a.txt",
		Folder:       testFolder,
		PartId:       2,
		Offset:       int64(len(corrupt)),
		Size:         len(good),
		ChecksumType: ChecksumCRC64NVME,
		Checksum:     testPartCRC64,
	}
	insertPart(t, st, part2)

	expectNoMessages(t, checkPart(t, st, part2))

	// and the corrupt part 1, over the same file, must still be caught
	part1 := DBPartRegular{
		FileId:   "file-0001",
		Project:  "project-0001",
		FileName: "a.txt",
		Folder:   testFolder,
		PartId:   1,
		Offset:   0,
		Size:     len(corrupt),
		MD5:      testPartMD5,
	}
	insertPart(t, st, part1)
	expectOneMessageContaining(t, checkPart(t, st, part1), "md5sum mismatch")
}

// Trailing bytes beyond the part must not be pulled into the digest.
func TestCheckPartIgnoresTrailingBytes(t *testing.T) {
	st := newTestState(t)

	writePartFile(t, "a.txt", []byte(testPartContent), []byte("trailing garbage"))

	p := wholeFilePart("a.txt", len(testPartContent))
	p.ChecksumType = ChecksumCRC64NVME
	p.Checksum = testPartCRC64
	insertPart(t, st, p)

	expectNoMessages(t, checkPart(t, st, p))
}

// A part larger than the internal copy buffer exercises the streaming path over
// many chunks, in both the matching and the corrupt case.
func TestCheckPartLargePartStreaming(t *testing.T) {
	content := bytes.Repeat([]byte("dxda"), 512*1024) // 2 MiB

	checksum, err := CalculateChecksum(ChecksumCRC64NVME, content)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	t.Run("match", func(t *testing.T) {
		st := newTestState(t)
		p := wholeFilePart("big.txt", len(content))
		p.ChecksumType = ChecksumCRC64NVME
		p.Checksum = checksum
		writePartFile(t, p.FileName, content)
		insertPart(t, st, p)

		expectNoMessages(t, checkPart(t, st, p))
	})

	t.Run("corrupt final byte", func(t *testing.T) {
		st := newTestState(t)
		corrupt := bytes.Clone(content)
		corrupt[len(corrupt)-1] = 'X'

		p := wholeFilePart("big.txt", len(content))
		p.ChecksumType = ChecksumCRC64NVME
		p.Checksum = checksum
		writePartFile(t, p.FileName, corrupt)
		insertPart(t, st, p)

		expectOneMessageContaining(t, checkPart(t, st, p), "checksum mismatch")
	})
}

// A zero-length part has a well-defined checksum and must verify cleanly.
func TestCheckPartEmptyPart(t *testing.T) {
	st := newTestState(t)

	checksum, err := CalculateChecksum(ChecksumCRC64NVME, nil)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	p := wholeFilePart("empty.txt", 0)
	p.ChecksumType = ChecksumCRC64NVME
	p.Checksum = checksum
	writePartFile(t, p.FileName, nil)
	insertPart(t, st, p)

	expectNoMessages(t, checkPart(t, st, p))
}

// The database must carry the checksum type and the per-part checksum through
// from the manifest, otherwise the integrity check has nothing to work with.
func TestCreateManifestDBStoresChecksumColumns(t *testing.T) {
	st := newTestState(t)

	checksumType := ChecksumCRC64NVME
	checksum := testPartCRC64
	manifest := Manifest{Files: []DXFile{
		DXFileRegular{
			Folder:       testFolder,
			Id:           "file-0002",
			ProjId:       "project-0001",
			Name:         "withChecksum.txt",
			Size:         int64(len(testPartContent)),
			ChecksumType: &checksumType,
			Parts:        []DXPart{{Id: 1, Size: len(testPartContent), Checksum: &checksum}},
		},
		DXFileRegular{
			Folder: testFolder,
			Id:     "file-0003",
			ProjId: "project-0001",
			Name:   "md5Only.txt",
			Size:   int64(len(testPartContent)),
			Parts:  []DXPart{{Id: 1, Size: len(testPartContent), MD5: testPartMD5}},
		},
	}}
	// newTestState already created the schema, so only the rows are added here
	txn, err := st.db.Begin()
	if err != nil {
		t.Fatalf("Could not begin a transaction: %s", err)
	}
	for _, f := range manifest.Files {
		st.addRegularFileToTable(txn, f.(DXFileRegular))
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("Could not commit: %s", err)
	}

	for _, tc := range []struct{ fileId, wantType, wantChecksum, wantMD5 string }{
		{"file-0002", ChecksumCRC64NVME, testPartCRC64, ""},
		{"file-0003", "", "", testPartMD5},
	} {
		var gotType, gotChecksum, gotMD5 string
		err := st.db.QueryRow(
			"SELECT checksum_type, checksum, md5 FROM manifest_regular_stats WHERE file_id = ?",
			tc.fileId).Scan(&gotType, &gotChecksum, &gotMD5)
		if err != nil {
			t.Fatalf("%s: could not read the row: %s", tc.fileId, err)
		}
		if gotType != tc.wantType || gotChecksum != tc.wantChecksum || gotMD5 != tc.wantMD5 {
			t.Errorf("%s: stored (type=%q checksum=%q md5=%q), expected (type=%q checksum=%q md5=%q)",
				tc.fileId, gotType, gotChecksum, gotMD5, tc.wantType, tc.wantChecksum, tc.wantMD5)
		}
	}
}

// A database created before newer columns/tables existed should be migrated
// in-place and accepted.
func TestCheckSchemaVersion(t *testing.T) {
	st := newTestState(t)

	if err := st.CheckSchemaVersion(); err != nil {
		t.Errorf("Expected the current schema to be accepted, got %v", err)
	}

	if _, err := st.db.Exec("DROP TABLE manifest_regular_chunk_stats"); err != nil {
		t.Fatalf("Could not drop chunk table: %s", err)
	}

	if _, err := st.db.Exec("DROP TABLE manifest_regular_stats"); err != nil {
		t.Fatalf("Could not drop the table: %s", err)
	}
	// the pre-checksum schema
	if _, err := st.db.Exec(`CREATE TABLE manifest_regular_stats (
		file_id text, project text, name text, folder text, part_id integer,
		offset integer, size integer, md5 text, bytes_fetched integer,
		download_done_time integer
	)`); err != nil {
		t.Fatalf("Could not create the old table: %s", err)
	}

	if err := st.CheckSchemaVersion(); err != nil {
		t.Fatalf("Expected legacy schema to be auto-migrated, got %v", err)
	}

	for _, col := range []string{"checksum_type", "checksum", "split_mode", "integrity_done"} {
		hasCol, err := hasColumn(st.db, "manifest_regular_stats", col)
		if err != nil {
			t.Fatalf("Could not inspect column %s: %s", col, err)
		}
		if !hasCol {
			t.Fatalf("Expected migrated column %s to exist", col)
		}
	}

	hasChunkTable, err := tableExists(st.db, "manifest_regular_chunk_stats")
	if err != nil {
		t.Fatalf("Could not inspect chunk table: %s", err)
	}
	if !hasChunkTable {
		t.Fatalf("Expected chunk table to be created by migration")
	}

	for _, idx := range []string{
		"idx_regular_chunk_incomplete",
		"idx_regular_chunk_file_part",
		"idx_regular_stats_split_incomplete",
	} {
		var count int
		err := st.db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?", idx).Scan(&count)
		if err != nil {
			t.Fatalf("Could not inspect index %s: %s", idx, err)
		}
		if count != 1 {
			t.Fatalf("Expected index %s to exist after migration", idx)
		}
	}
}

func TestAddRegularFileToTableChunkedWhenEnabled(t *testing.T) {
	os.Setenv("DXDA_ENABLE_REGULAR_PART_CHUNKING", "1")
	os.Setenv("DXDA_REGULAR_PART_SPLIT_THRESHOLD_BYTES", "1")
	t.Cleanup(func() {
		os.Unsetenv("DXDA_ENABLE_REGULAR_PART_CHUNKING")
		os.Unsetenv("DXDA_REGULAR_PART_SPLIT_THRESHOLD_BYTES")
	})

	st := newTestState(t)

	bigPart := 20 * MiB
	reg := DXFileRegular{
		Folder: testFolder,
		Id:     "file-big-0001",
		ProjId: "project-0001",
		Name:   "big.bin",
		Parts: []DXPart{
			{Id: 1, Size: bigPart, MD5: testPartMD5},
		},
	}

	txn, err := st.db.Begin()
	if err != nil {
		t.Fatalf("Could not begin transaction: %s", err)
	}
	st.addRegularFileToTable(txn, reg)
	if err := txn.Commit(); err != nil {
		t.Fatalf("Could not commit transaction: %s", err)
	}

	var splitMode string
	if err := st.db.QueryRow(
		"SELECT split_mode FROM manifest_regular_stats WHERE file_id = ? AND part_id = 1",
		reg.Id,
	).Scan(&splitMode); err != nil {
		t.Fatalf("Could not read split mode: %s", err)
	}
	if splitMode != splitModeChunked {
		t.Fatalf("Expected split mode %q, got %q", splitModeChunked, splitMode)
	}

	var numChunks int
	if err := st.db.QueryRow(
		"SELECT COUNT(*) FROM manifest_regular_chunk_stats WHERE file_id = ? AND part_id = 1",
		reg.Id,
	).Scan(&numChunks); err != nil {
		t.Fatalf("Could not count chunk rows: %s", err)
	}
	if numChunks < 2 {
		t.Fatalf("Expected at least 2 chunk rows for large part, got %d", numChunks)
	}
}

func TestAddRegularFileToTableNotChunkedWhenDisabled(t *testing.T) {
	os.Setenv("DXDA_ENABLE_REGULAR_PART_CHUNKING", "0")
	os.Setenv("DXDA_REGULAR_PART_SPLIT_THRESHOLD_BYTES", "1")
	t.Cleanup(func() {
		os.Unsetenv("DXDA_ENABLE_REGULAR_PART_CHUNKING")
		os.Unsetenv("DXDA_REGULAR_PART_SPLIT_THRESHOLD_BYTES")
	})

	st := newTestState(t)

	reg := DXFileRegular{
		Folder: testFolder,
		Id:     "file-big-0002",
		ProjId: "project-0001",
		Name:   "big2.bin",
		Parts: []DXPart{
			{Id: 1, Size: 20 * MiB, MD5: testPartMD5},
		},
	}

	txn, err := st.db.Begin()
	if err != nil {
		t.Fatalf("Could not begin transaction: %s", err)
	}
	st.addRegularFileToTable(txn, reg)
	if err := txn.Commit(); err != nil {
		t.Fatalf("Could not commit transaction: %s", err)
	}

	var splitMode string
	if err := st.db.QueryRow(
		"SELECT split_mode FROM manifest_regular_stats WHERE file_id = ? AND part_id = 1",
		reg.Id,
	).Scan(&splitMode); err != nil {
		t.Fatalf("Could not read split mode: %s", err)
	}
	if splitMode != splitModePartRow {
		t.Fatalf("Expected split mode %q, got %q", splitModePartRow, splitMode)
	}

	var numChunks int
	if err := st.db.QueryRow(
		"SELECT COUNT(*) FROM manifest_regular_chunk_stats WHERE file_id = ? AND part_id = 1",
		reg.Id,
	).Scan(&numChunks); err != nil {
		t.Fatalf("Could not count chunk rows: %s", err)
	}
	if numChunks != 0 {
		t.Fatalf("Expected 0 chunk rows when chunking is disabled, got %d", numChunks)
	}
}

func TestFinalizeChunkedRegularPartIntegrityFailureResetsChunkRows(t *testing.T) {
	os.Setenv("DXDA_ENABLE_REGULAR_PART_CHUNKING", "1")
	os.Setenv("DXDA_REGULAR_PART_SPLIT_THRESHOLD_BYTES", "1")
	t.Cleanup(func() {
		os.Unsetenv("DXDA_ENABLE_REGULAR_PART_CHUNKING")
		os.Unsetenv("DXDA_REGULAR_PART_SPLIT_THRESHOLD_BYTES")
	})

	st := newTestState(t)
	st.maxChunkSize = 8

	goodContent := bytes.Repeat([]byte("A"), 40)
	corruptContent := bytes.Repeat([]byte("B"), 40)
	sum := md5.Sum(goodContent)
	expectedMD5 := hex.EncodeToString(sum[:])

	reg := DXFileRegular{
		Folder: testFolder,
		Id:     "file-chunked-reset-0001",
		ProjId: "project-0001",
		Name:   "chunked-reset.bin",
		Parts: []DXPart{
			{Id: 1, Size: len(goodContent), MD5: expectedMD5},
		},
	}

	writePartFile(t, reg.Name, corruptContent)

	txn, err := st.db.Begin()
	if err != nil {
		t.Fatalf("Could not begin transaction: %s", err)
	}
	st.addRegularFileToTable(txn, reg)
	if err := txn.Commit(); err != nil {
		t.Fatalf("Could not commit transaction: %s", err)
	}

	var splitMode string
	if err := st.db.QueryRow(
		"SELECT split_mode FROM manifest_regular_stats WHERE file_id = ? AND part_id = 1",
		reg.Id,
	).Scan(&splitMode); err != nil {
		t.Fatalf("Could not read split mode: %s", err)
	}
	if splitMode != splitModeChunked {
		t.Fatalf("Expected split mode %q, got %q", splitModeChunked, splitMode)
	}

	var chunkRows int
	if err := st.db.QueryRow(
		"SELECT COUNT(*) FROM manifest_regular_chunk_stats WHERE file_id = ? AND part_id = 1",
		reg.Id,
	).Scan(&chunkRows); err != nil {
		t.Fatalf("Could not count chunk rows: %s", err)
	}
	if chunkRows < 2 {
		t.Fatalf("Expected multiple chunk rows, got %d", chunkRows)
	}

	if _, err := st.db.Exec(
		"UPDATE manifest_regular_chunk_stats SET bytes_fetched = size, download_done_time = 1 WHERE file_id = ? AND part_id = 1",
		reg.Id,
	); err != nil {
		t.Fatalf("Could not mark chunk rows complete: %s", err)
	}

	st.maybeFinalizeChunkedRegularPart(reg.Id, 1, 123)

	var parentBytes int
	var integrityDone int
	if err := st.db.QueryRow(
		"SELECT bytes_fetched, integrity_done FROM manifest_regular_stats WHERE file_id = ? AND part_id = 1",
		reg.Id,
	).Scan(&parentBytes, &integrityDone); err != nil {
		t.Fatalf("Could not read parent row state: %s", err)
	}
	if parentBytes != 0 || integrityDone != 0 {
		t.Fatalf("Expected parent row reset after integrity failure, got bytes_fetched=%d integrity_done=%d", parentBytes, integrityDone)
	}

	var remainingCompleted int
	if err := st.db.QueryRow(
		"SELECT COUNT(*) FROM manifest_regular_chunk_stats WHERE file_id = ? AND part_id = 1 AND bytes_fetched = size",
		reg.Id,
	).Scan(&remainingCompleted); err != nil {
		t.Fatalf("Could not count completed chunks after reset: %s", err)
	}
	if remainingCompleted != 0 {
		t.Fatalf("Expected all chunk rows to be reset, still have %d completed chunks", remainingCompleted)
	}
}

func TestPerFileInflightChunkCap(t *testing.T) {
	st := newTestState(t)
	st.maxInflightChunksPerFile = 2

	const totalWorkers = 6
	fileID := "file-cap-0001"

	releaseGate := make(chan struct{})
	acquired := make(chan struct{}, totalWorkers)
	done := make(chan struct{}, totalWorkers)

	var mu sync.Mutex
	inFlight := 0
	maxSeen := 0

	for i := 0; i < totalWorkers; i++ {
		go func() {
			releasePermit := st.acquireRegularChunkPermit(fileID)

			mu.Lock()
			inFlight += 1
			if inFlight > maxSeen {
				maxSeen = inFlight
			}
			mu.Unlock()

			acquired <- struct{}{}
			<-releaseGate

			mu.Lock()
			inFlight -= 1
			mu.Unlock()

			releasePermit()
			done <- struct{}{}
		}()
	}

	// Exactly `cap` goroutines should acquire permits immediately.
	for i := 0; i < st.maxInflightChunksPerFile; i++ {
		<-acquired
	}

	// Without releasing a permit, no additional goroutine should acquire one.
	for i := 0; i < 1000; i++ {
		select {
		case <-acquired:
			t.Fatalf("Per-file cap exceeded: acquired more than %d concurrent chunk permits", st.maxInflightChunksPerFile)
		default:
			runtime.Gosched()
		}
	}

	close(releaseGate)

	for i := 0; i < totalWorkers; i++ {
		<-done
	}

	if maxSeen > st.maxInflightChunksPerFile {
		t.Fatalf("Per-file cap exceeded: max concurrent=%d cap=%d", maxSeen, st.maxInflightChunksPerFile)
	}
}

func TestResolveMaxInflightChunksPerFile(t *testing.T) {
	if got := resolveMaxInflightChunksPerFile("", 24, 32); got != 16 {
		t.Fatalf("Expected auto cap 16 for empty env, got %d", got)
	}

	if got := resolveMaxInflightChunksPerFile("auto", 12, 8); got != 8 {
		t.Fatalf("Expected auto cap bounded by numThreads (8), got %d", got)
	}

	if got := resolveMaxInflightChunksPerFile("6", 32, 8); got != 6 {
		t.Fatalf("Expected explicit cap 6, got %d", got)
	}

	if got := resolveMaxInflightChunksPerFile("100", 32, 7); got != 7 {
		t.Fatalf("Expected explicit cap bounded by numThreads (7), got %d", got)
	}

	if got := resolveMaxInflightChunksPerFile("bad", 10, 20); got != 10 {
		t.Fatalf("Expected invalid env to fall back to auto cap 10, got %d", got)
	}
}
