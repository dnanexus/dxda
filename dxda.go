package dxda

// Some inspiration + code snippets taken from https://github.com/dnanexus/precision-fda/blob/master/go/pfda.go
//
// TODO: add more unit tests, setup deeper integration tests
//
import (
	"bytes"
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"runtime"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3" // Following canonical example on go-sqlite3 'simple.go'
)

const (
	// Range for the number of threads we want to use
	minNumThreads = 2
	maxNumThreads = 32

	numRetries                     = 10
	numRetriesChecksumMismatch     = 3
	secondsInYear              int = 60 * 60 * 24 * 365
)

var err error

// DXDownloadURL ...
type DXDownloadURL struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

// a part to be downloaded. Can be:
// 1) part of a regular file
// 2) part of symbolic link (a web address)
type DBPart interface {
	folder() string
	fileId() string
	fileName() string
	offset() int64
	project() string
	size() int
}

// Part of a dnanexus file
type DBPartRegular struct {
	FileId           string
	Project          string
	FileName         string
	Folder           string
	PartId           int
	Offset           int64
	Size             int
	MD5              string
	BytesFetched     int
	DownloadDoneTime int64 // The time when it completed downloading
}

func (reg DBPartRegular) folder() string   { return reg.Folder }
func (reg DBPartRegular) fileName() string { return reg.FileName }
func (reg DBPartRegular) fileId() string   { return reg.FileId }
func (reg DBPartRegular) project() string  { return reg.Project }
func (reg DBPartRegular) offset() int64    { return reg.Offset }
func (reg DBPartRegular) size() int        { return reg.Size }

// symlink parts do not have checksum. There is only a
// global MD5 checksum on the entire file. There is also
// no need to get a pre-auth URL for the file
type DBPartSymlink struct {
	FileId           string
	Project          string
	FileName         string
	Folder           string
	PartId           int
	Offset           int64
	Size             int
	BytesFetched     int
	DownloadDoneTime int64 // The time when it completed downloading
	Url              string
}

func (slnk DBPartSymlink) folder() string   { return slnk.Folder }
func (slnk DBPartSymlink) fileName() string { return slnk.FileName }
func (slnk DBPartSymlink) fileId() string   { return slnk.FileId }
func (slnk DBPartSymlink) project() string  { return slnk.Project }
func (slnk DBPartSymlink) offset() int64    { return slnk.Offset }
func (slnk DBPartSymlink) size() int        { return slnk.Size }

// JobInfo ...
type JobInfo struct {
	part       DBPart
	url        *DXDownloadURL
	completeNs int64
}

// DownloadStatus ...
type DownloadStatus struct {
	NumParts         int64
	NumBytes         int64
	NumPartsComplete int64
	NumBytesComplete int64

	// periodicity of progress report
	ProgressInterval time.Duration

	// Size of window in nanoseconds where to look for
	// completed downloads
	MaxWindowSize int64
}

type State struct {
	dxEnv           DXEnvironment
	opts            Opts
	mutex           sync.Mutex
	db              *sql.DB
	ds              *DownloadStatus // only the progress report thread accesses this field
	timeOfLastError int
	maxChunkSize    int64
}

//-----------------------------------------------------------------

// Utilities to interact with the DNAnexus API
// TODO: Create automatic API wrappers for the dx toolkit
// e.g. via: https://github.com/dnanexus/dx-toolkit/tree/master/src/api_wrappers

// The user didn't specify how many threads to use, so we can
// figure out the optimal number on our own.
//
// Constraints:
// 1. Don't use more than twice the number of cores
// 2. Leave at least 1GiB of RAM for the machine
func calcNumThreads(maxChunkSize int64) int {
	numCPUs := runtime.NumCPU()
	hwMemoryBytes := memorySizeBytes()

	fmt.Printf("number of machine cores: %d\n", numCPUs)
	fmt.Printf("memory size: %d GiB\n", hwMemoryBytes/GiB)

	numThreads := MinInt(2*numCPUs, maxNumThreads)
	memoryCostPerThread := 3 * int64(maxChunkSize)

	for numThreads > minNumThreads {
		projectedMemoryUseBytes := memoryCostPerThread * int64(numThreads)
		if projectedMemoryUseBytes < (hwMemoryBytes - GiB) {
			return numThreads
		}
		numThreads -= 2
	}
	return minNumThreads
}

// Initialize the state
func NewDxDa(dxEnv DXEnvironment, fname string, optsRaw Opts) *State {
	statsFname := fname + ".stats.db?_busy_timeout=60000&cache=shared&mode=rwc"
	db, err := sql.Open("sqlite3", statsFname)
	check(err)
	db.SetMaxOpenConns(1)

	// Constraints:
	// 1) The chunk should be large enough to optimize the http(s) network stack
	// 2) The chunk should be small enough to be able to take it in one request,
	//    causing minimal retries.
	maxChunkSize := int64(0)
	if dxEnv.DxJobId == "" {
		// a remote machine, with a lower bandwith network connection
		maxChunkSize = 16 * MiB
	} else {
		// A cloud worker with good bandwitdh
		maxChunkSize = 64 * MiB
		//maxChunkSize = 128 * MiB
	}

	// if the number of threads isn't set we
	// need to calculate it and modify the options
	opts := optsRaw
	if opts.NumThreads == 0 {
		opts.NumThreads = calcNumThreads(maxChunkSize)
	}

	// Limit the number of threads
	fmt.Printf("Downloading files using %d threads\n", opts.NumThreads)
	fmt.Printf("maximal memory chunk size: %d MiB\n", maxChunkSize/MiB)
	//	runtime.GOMAXPROCS(st.opts.NumThreads + 2)

	return &State{
		dxEnv:           dxEnv,
		opts:            opts,
		mutex:           sync.Mutex{},
		db:              db,
		ds:              nil,
		timeOfLastError: 0,
		maxChunkSize:    maxChunkSize,
	}
}

func (st *State) Close() {
	st.db.Close()
}

// Probably a better way to do this :)
func (st *State) queryDBIntegerResult(query string) int64 {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	rows, err := st.db.Query(query)
	check(err)

	var cnt int64
	rows.Next()
	rows.Scan(&cnt)
	rows.Close()

	return cnt
}

// DiskSpaceString ...
// write the number of bytes as a human readable string
func diskSpaceString(numBytes int64) string {
	const KB = 1024
	const MB = 1024 * KB
	const GB = 1024 * MB
	const TB = 1024 * GB

	tb := float64(numBytes) / float64(TB)
	if tb >= 1.0 {
		return fmt.Sprintf("%.1fTB", tb)
	}

	gb := float64(numBytes) / float64(GB)
	if gb >= 1.0 {
		return fmt.Sprintf("%.1fGB", gb)
	}

	mb := float64(numBytes) / float64(MB)
	if mb >= 1.0 {
		return fmt.Sprintf("%.1fMB", mb)
	}
	return fmt.Sprintf("%dBytes", numBytes)
}

// CheckDiskSpace ...
// Check that we have enough disk space for all downloaded files
func (st *State) CheckDiskSpace() error {
	// Calculate total disk space required. To get an accurate number,
	// query the database, and sum the space for missing pieces.
	//
	totalSizeBytes :=
		st.queryDBIntegerResult("SELECT SUM(size) FROM manifest_regular_stats WHERE bytes_fetched != size") +
			st.queryDBIntegerResult("SELECT SUM(size) FROM manifest_symlink_stats WHERE bytes_fetched != size")

	// Find how much local disk space is available
	var stat syscall.Statfs_t
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	if err := syscall.Statfs(wd, &stat); err != nil {
		return err
	}

	// Available blocks * size per block = available space in bytes
	availableBytes := int64(stat.Bavail) * int64(stat.Bsize)
	if availableBytes < totalSizeBytes {
		desc := fmt.Sprintf("Not enough disk space, available = %s, required = %s",
			diskSpaceString(availableBytes),
			diskSpaceString(totalSizeBytes))
		return errors.New(desc)
	}
	diskSpaceStr := fmt.Sprintf("Required disk space = %s, available = %s\n",
		diskSpaceString(totalSizeBytes),
		diskSpaceString(availableBytes))
	PrintLogAndOut(diskSpaceStr)
	return nil
}

func (st *State) addRegularFileToTable(txn *sql.Tx, f DXFileRegular) {
	offset := int64(0)
	for _, p := range f.Parts {
		sqlStmt := fmt.Sprintf(`
				INSERT INTO manifest_regular_stats
				VALUES ('%s', '%s', '%s', '%s', %d, '%d', '%d', '%s', '%d', '%d');
				`,
			f.Id, f.ProjId, f.Name, f.Folder, p.Id, offset, p.Size, p.MD5, 0, 0)
		_, err := txn.Exec(sqlStmt)
		check(err)
		offset += int64(p.Size)
	}
}

func (st *State) addSymlinkToTable(txn *sql.Tx, slnk DXFileSymlink) {
	// split the symbolic link into chunks, and download several in parallel
	offset := int64(0)
	pId := 1

	for offset < slnk.Size {
		// make sure we don't go over the file size
		endOfs := MinInt64(offset+st.maxChunkSize, slnk.Size)
		partLen := endOfs - offset

		if partLen <= 0 {
			panic(fmt.Sprintf("part length could not be zero or less (%d)", partLen))
		}
		sqlStmt := fmt.Sprintf(`
				INSERT INTO manifest_symlink_stats
				VALUES ('%s', '%s', '%s', '%s', '%d', '%d', '%d', '%d', '%d');
				`,
			slnk.Id, slnk.ProjId, slnk.Name, slnk.Folder, pId, offset, partLen, 0, 0)
		_, err := txn.Exec(sqlStmt)
		check(err)
		offset += partLen
		pId += 1
	}

	// add to global table
	sqlStmt := fmt.Sprintf(`
		INSERT INTO symlinks
		VALUES ('%s', '%s', '%s', '%s', '%d', '%s');
		`,
		slnk.Folder, slnk.Id, slnk.ProjId, slnk.Name, slnk.Size, slnk.MD5)
	_, err := txn.Exec(sqlStmt)
	check(err)
}

// Read the manifest file, and build a database with an empty state
// for each part in each file.
func (st *State) CreateManifestDB(manifest Manifest, fname string) {
	statsFname := fname + ".stats.db?_busy_timeout=60000&cache=shared&mode=rwc"
	os.Remove(statsFname)
	// db, err := sql.Open("sqlite3", statsFname)
	// check(err)
	// defer db.Close()

	sqlStmt := `
	CREATE TABLE manifest_regular_stats (
		file_id text,
		project text,
		name text,
		folder text,
		part_id integer,
                offset integer,
		size integer,
		md5 text,
		bytes_fetched integer,
                download_done_time integer
	);
	`
	_, err = st.db.Exec(sqlStmt)
	check(err)

	// symbolic link parts do not have md5 checksums
	sqlStmt = `
	CREATE TABLE manifest_symlink_stats (
		file_id text,
		project text,
		name text,
		folder text,
		part_id integer,
                offset integer,
		size integer,
		bytes_fetched integer,
                download_done_time integer
	);
	`
	_, err = st.db.Exec(sqlStmt)
	check(err)

	// create a table for all the symlinks. This is the place
	// to record their overall file checksum. We can't put it
	// in the per-chunk table.
	sqlStmt = `
	CREATE TABLE symlinks (
  	        folder  text,
           	id      text,
	        proj_id text,
  	        name    text,
	        size    integer,
	        md5     text
	);
	`
	_, err = st.db.Exec(sqlStmt)
	check(err)

	txn, err := st.db.Begin()
	check(err)

	// Regular files
	for _, f := range manifest.Files {
		switch f.(type) {
		case DXFileRegular:
			st.addRegularFileToTable(txn, f.(DXFileRegular))
		case DXFileSymlink:
			st.addSymlinkToTable(txn, f.(DXFileSymlink))
		}
	}

	err = txn.Commit()
	check(err)

	// TODO Log network settings and other helpful info for debugging
	PrintLogAndOut("Preparing files for download\n")
	st.prepareFilesForDownload(manifest)
}

// create an empty file for each download path.
//
// TODO: Optimize this for only files that need to be downloaded
func (st *State) prepareFilesForDownload(m Manifest) {
	for _, f := range m.Files {
		// Create directory structure and initialize file if it doesn't exist
		folder := path.Join("./", f.folder())
		fname := path.Join(folder, f.name())
		if _, err := os.Stat(fname); os.IsNotExist(err) {
			err := os.MkdirAll(folder, 0777)
			check(err)
			localf, err := os.Create(fname)
			check(err)
			localf.Close()
		}
	}
}

// InitDownloadStatus ...
func (st *State) InitDownloadStatus() {
	// total amounts to download, calculated once
	numParts :=
		st.queryDBIntegerResult("SELECT COUNT(*) FROM manifest_regular_stats") +
			st.queryDBIntegerResult("SELECT COUNT(*) FROM manifest_symlink_stats")
	numBytes :=
		st.queryDBIntegerResult("SELECT SUM(size) FROM manifest_regular_stats") +
			st.queryDBIntegerResult("SELECT SUM(size) FROM manifest_symlink_stats")

	progressIntervalSec := 0
	if st.dxEnv.DxJobId == "" {
		// interactive use, we want to see frequent updates
		progressIntervalSec = 5
	} else {
		// Non interactive use: we don't want to fill the logs with
		// too many messages
		progressIntervalSec = 15
	}

	st.ds = &DownloadStatus{
		NumParts:         numParts,
		NumBytes:         numBytes,
		NumPartsComplete: 0,
		NumBytesComplete: 0,
		ProgressInterval: time.Duration(progressIntervalSec) * time.Second,
		MaxWindowSize:    int64(2 * 60 * 1000 * 1000 * 1000),
	}

	if st.opts.Verbose {
		log.Printf("init download status %v\n", st.ds)
	}
}

// Calculate bandwidth in MB/sec. Query the database, and find
// all recently completed downloaded parts.
func (st *State) calcBandwidth(timeWindowNanoSec int64) float64 {
	if timeWindowNanoSec < 1000 {
		// sanity check; if the interval is very short, just return zero.
		return 0.0
	}

	// Time and time window measured in nano seconds
	now := time.Now().UnixNano()
	lowerBound := now - timeWindowNanoSec

	queryReg := fmt.Sprintf(
		"SELECT SUM(bytes_fetched) FROM manifest_regular_stats WHERE download_done_time > %d",
		lowerBound)
	regBytesDownloadedInTimeWindow := st.queryDBIntegerResult(queryReg)

	querySlnk := fmt.Sprintf(
		"SELECT SUM(bytes_fetched) FROM manifest_symlink_stats WHERE download_done_time > %d",
		lowerBound)
	slnkBytesDownloadedInTimeWindow := st.queryDBIntegerResult(querySlnk)

	bytesDownloadedInTimeWindow := regBytesDownloadedInTimeWindow + slnkBytesDownloadedInTimeWindow

	// convert to megabytes downloaded divided by seconds
	mbDownloaded := float64(bytesDownloadedInTimeWindow) / float64(1024*1024)
	timeDeltaSec := float64(timeWindowNanoSec) / float64(1000*1000*1000)
	return mbDownloaded / timeDeltaSec
}

func bytes2MiB(bytes int64) int64 {
	return bytes / MiB
}

// DownloadProgressOneTime ...
// Report on progress so far
func (st *State) DownloadProgressOneTime(timeWindowNanoSec int64) string {
	// query the current progress
	st.ds.NumBytesComplete =
		st.queryDBIntegerResult("SELECT SUM(bytes_fetched) FROM manifest_regular_stats WHERE bytes_fetched = size") +
			st.queryDBIntegerResult("SELECT SUM(bytes_fetched) FROM manifest_symlink_stats WHERE bytes_fetched = size")
	st.ds.NumPartsComplete =
		st.queryDBIntegerResult("SELECT COUNT(*) FROM manifest_regular_stats WHERE bytes_fetched = size") +
			st.queryDBIntegerResult("SELECT COUNT(*) FROM manifest_symlink_stats WHERE bytes_fetched = size")

	// calculate bandwitdh
	bandwidthMBSec := st.calcBandwidth(timeWindowNanoSec)

	// report on GC statistics
	gcReport := ""
	if st.opts.GcInfo {
		var gcStats runtime.MemStats
		runtime.ReadMemStats(&gcStats)
		crntAlloc := int64(gcStats.Alloc)
		totalAlloc := int64(gcStats.TotalAlloc)
		pauseNs := gcStats.PauseTotalNs
		numGcCycles := gcStats.NumGC

		gcReport = fmt.Sprintf("   GC (alloc=%d/%d MB, pause=%d ms, #cycles=%d)",
			bytes2MiB(crntAlloc), bytes2MiB(totalAlloc),
			pauseNs/1e6, numGcCycles)
	}
	desc := fmt.Sprintf("Downloaded %d/%d MB\t%d/%d Parts (~%.1f MB/s written to disk estimated over the last %ds)%s",
		bytes2MiB(st.ds.NumBytesComplete), bytes2MiB(st.ds.NumBytes),
		st.ds.NumPartsComplete, st.ds.NumParts,
		bandwidthMBSec,
		timeWindowNanoSec/1e9,
		gcReport)

	return desc
}

// A loop that reports on download progress periodically.
func (st *State) downloadProgressContinuous(wg *sync.WaitGroup) {
	// Start time of the measurements, in nano seconds
	startTime := time.Now()
	lastReportTs := startTime

	// only the progress-report thread has access to the "ds" state
	// field. That is why no locking is required here.
	for true {
		// Sleep for a number of seconds, so as to not flood the screen
		// with messages. This also substantially limits the number
		// of database queries.
		time.Sleep(1 * time.Second)
		if st.ds.NumPartsComplete >= st.ds.NumParts {
			// signal that the thread is done
			wg.Done()
			return
		}

		now := time.Now()
		if now.Before(lastReportTs.Add(st.ds.ProgressInterval)) {
			continue
		}
		lastReportTs = now

		// If we just started the measurements, we have a short
		// history to examine. Limit the window size accordingly.
		deltaNanoSec := now.UnixNano() - startTime.UnixNano()
		if deltaNanoSec > st.ds.MaxWindowSize {
			deltaNanoSec = st.ds.MaxWindowSize
		}
		desc := st.DownloadProgressOneTime(deltaNanoSec)

		if st.dxEnv.DxJobId == "" {
			// running on a console, erase the previous line
			// TODO: Get rid of this temporary space-padding fix for carriage returns
			fmt.Printf("                                                                      \r")
			fmt.Printf("%s\r", desc)
		} else {
			// We are on a dx-job, and we want to see the history of printouts
			// Note: the "\r" character causes problems in job logs, so do not use it.
			fmt.Printf("%s\n", desc)
		}
	}
}

// Download part of a symlink
func (st *State) downloadSymlinkPart(
	httpClient *http.Client,
	p DBPartSymlink,
	u DXDownloadURL,
	memoryBuf []byte) error {

	if st.opts.Verbose {
		log.Printf("downloadSymlinkPart %v %v\n", p, u)
	}

	fname := fmt.Sprintf(".%s/%s", p.folder(), p.fileName())
	localf, err := os.OpenFile(fname, os.O_WRONLY, 0777)
	check(err)
	defer localf.Close()

	headers := make(map[string]string)
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", p.offset(), p.offset()+int64(p.size())-1)

	for k, v := range u.Headers {
		headers[k] = v
	}
	err = DxHttpRequestData(context.TODO(), httpClient, "GET", u.URL, headers, []byte("{}"), p.Size, memoryBuf)
	check(err)
	body := memoryBuf[:p.Size]

	_, err = localf.WriteAt(body, p.offset())
	check(err)

	return nil
}

// Download part of a file and verify its checksum in memory
//
func (st *State) downloadRegPartCheckSum(
	httpClient *http.Client,
	p DBPartRegular,
	u DXDownloadURL,
	memoryBuf []byte) (bool, error) {

	if st.opts.Verbose {
		log.Printf("downloadRegPart %v %v\n", p, u)
	}

	fname := fmt.Sprintf(".%s/%s", p.folder(), p.fileName())
	localf, err := os.OpenFile(fname, os.O_WRONLY, 0777)
	check(err)
	defer localf.Close()

	// compute the checksum as we go
	hasher := md5.New()

	// loop through the part, reading in chunk pieces
	endPart := p.Offset + int64(p.Size) - 1
	for ofs := p.Offset; ofs <= endPart; ofs += st.maxChunkSize {
		chunkEnd := MinInt64(ofs+st.maxChunkSize-1, endPart)
		chunkSize := int(chunkEnd - ofs + 1)

		headers := make(map[string]string)
		headers["Range"] = fmt.Sprintf("bytes=%d-%d", ofs, chunkEnd)
		for k, v := range u.Headers {
			headers[k] = v
		}

		err := DxHttpRequestData(context.TODO(), httpClient, "GET", u.URL, headers, []byte("{}"), chunkSize, memoryBuf)
		check(err)
		body := memoryBuf[:chunkSize]

		// write to disk
		_, err = localf.WriteAt(body, ofs)
		check(err)

		// update the checksum
		_, err = io.Copy(hasher, bytes.NewReader(body))
		check(err)
	}

	// verify the checksum
	diskSum := hex.EncodeToString(hasher.Sum(nil))
	if diskSum != p.MD5 {
		return false, nil
	}

	return true, nil
}

func (st *State) downloadRegPart(
	httpClient *http.Client,
	p DBPartRegular,
	u DXDownloadURL,
	memoryBuf []byte) error {

	for i := 0; i < numRetriesChecksumMismatch; i++ {
		ok, err := st.downloadRegPartCheckSum(httpClient, p, u, memoryBuf)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		log.Printf("MD5 string of part Id %d does not match stored MD5sum. Retrying.", p.PartId)
	}

	return fmt.Errorf("MD5 checksum mismatch for part %d url=%s. Gave up after %d attempts",
		p.PartId, u.URL, numRetriesChecksumMismatch)
}

// create a download url if one doesn't exist
func (st *State) createURL(p DBPart, urls map[string]DXDownloadURL, httpClient *http.Client) *DXDownloadURL {
	var u DXDownloadURL

	// check if we already have it
	u, ok := urls[p.fileId()]
	if ok {
		return &u
	}

	// a regular DNAx file. Requires generating a pre-authenticated download URL.
	payload := fmt.Sprintf("{\"project\": \"%s\", \"duration\": %d}",
		p.project(), secondsInYear)

	body, err := DxAPI(
		context.TODO(),
		httpClient,
		numRetries,
		&st.dxEnv,
		fmt.Sprintf("%s/download", p.fileId()),
		payload)
	check(err)

	if err := json.Unmarshal(body, &u); err != nil {
		log.Printf(err.Error())
		panic("Could not unmarshal response from dnanexus for download URL")
	}

	// record the pre-auth URL so we don't have to create it again
	urls[p.fileId()] = u
	return &u
}

// A thread that adds pre-authenticated urls to each jobs.
//
func (st *State) preauthUrlsWorker(jobs <-chan JobInfo, jobsWithUrls chan JobInfo) {
	httpClient := NewHttpClient()
	urls := make(map[string]DXDownloadURL)

	for j := range jobs {
		switch j.part.(type) {
		case DBPartRegular:
			p := j.part.(DBPartRegular)
			j.url = st.createURL(p, urls, httpClient)

		case DBPartSymlink:
			pLnk := j.part.(DBPartSymlink)
			j.url = st.createURL(pLnk, urls, httpClient)
		}

		jobsWithUrls <- j
	}

	// we are done adding URLs to each job.
	close(jobsWithUrls)
}

func (st *State) worker(id int, jobsWithUrls <-chan JobInfo, jobsDbUpdate chan JobInfo, wg *sync.WaitGroup) {
	// Create one http client per worker. This should, hopefully, allow
	// caching open TCP/HTTP connections, reducing startup times.
	httpClient := NewHttpClient()
	memoryBuf := make([]byte, st.maxChunkSize)

	for j := range jobsWithUrls {
		switch j.part.(type) {
		case DBPartRegular:
			p := j.part.(DBPartRegular)
			st.downloadRegPart(httpClient, p, *j.url, memoryBuf)

		case DBPartSymlink:
			pLnk := j.part.(DBPartSymlink)
			st.downloadSymlinkPart(httpClient, pLnk, *j.url, memoryBuf)
		}

		// move the jobs to the next phase, which is updating the database
		j.completeNs = time.Now().UnixNano()
		jobsDbUpdate <- j
	}
	wg.Done()
}

func (st *State) dbApplyBulkUpdates(completedJobs []JobInfo) {
	if len(completedJobs) == 0 {
		return
	}
	st.mutex.Lock()
	defer st.mutex.Unlock()

	txn, err := st.db.Begin()
	check(err)
	defer txn.Commit()

	for _, j := range completedJobs {
		st.updateDBPart(txn, j.part, j.completeNs)
	}
}

// update the database when a job completes
// Do this in bulk
func (st *State) dbUpdateWorker(jobsDbUpdate <-chan JobInfo, wg *sync.WaitGroup) {
	var accu []JobInfo
	for j := range jobsDbUpdate {
		accu = append(accu, j)
		if len(accu) == 10 {
			st.dbApplyBulkUpdates(accu)
			accu = make([]JobInfo, 0)
		}
	}
	st.dbApplyBulkUpdates(accu)

	wg.Done()
}

// Download all the files that are mentioned in the manifest.
func (st *State) DownloadManifestDB(fname string) {
	if st.opts.Verbose {
		log.Printf("DownloadManifestDB %s\n", fname)
	}
	st.timeOfLastError = time.Now().Second()

	// build a job-channel that will hold all the parts. If we make it too small,
	// we will block before creating the worker threads.
	cntReg := st.queryDBIntegerResult("SELECT COUNT(*) FROM manifest_regular_stats WHERE bytes_fetched != size")
	cntSlnk := st.queryDBIntegerResult("SELECT COUNT(*) FROM manifest_symlink_stats WHERE bytes_fetched != size")
	totNumJobs := cntReg + cntSlnk
	jobs := make(chan JobInfo, totNumJobs)

	// create a job for each incomplete data file part
	rows, err := st.db.Query("SELECT * FROM manifest_regular_stats WHERE bytes_fetched != size")
	check(err)

	numRows := 0
	for rows.Next() {
		var p DBPartRegular
		err := rows.Scan(&p.FileId, &p.Project, &p.FileName, &p.Folder, &p.PartId, &p.Offset,
			&p.Size, &p.MD5, &p.BytesFetched, &p.DownloadDoneTime)
		check(err)
		j := JobInfo{
			part: p,
			url:  nil,
		}
		jobs <- j
		numRows++
	}
	rows.Close()
	if st.opts.Verbose {
		log.Printf("There are %d regular file pieces\n", numRows)
	}

	// create a job for each imcomplete data symlink part
	rows, err = st.db.Query("SELECT * FROM manifest_symlink_stats WHERE bytes_fetched != size")
	check(err)

	for rows.Next() {
		var p DBPartSymlink
		err = rows.Scan(&p.FileId, &p.Project, &p.FileName, &p.Folder, &p.PartId, &p.Offset,
			&p.Size, &p.BytesFetched, &p.DownloadDoneTime)
		check(err)
		j := JobInfo{
			part: p,
			url:  nil,
		}
		jobs <- j
	}
	rows.Close()

	// Close the job channel, there will be no more jobs.
	close(jobs)

	// the preauth thread adds a valid URL to each job.
	jobsWithUrls := make(chan JobInfo, totNumJobs)
	go st.preauthUrlsWorker(jobs, jobsWithUrls)

	// the db-update thread updates the database when jobs
	// complete.
	var wgDb sync.WaitGroup
	jobsDbUpdate := make(chan JobInfo, totNumJobs)
	wgDb.Add(1)
	go st.dbUpdateWorker(jobsDbUpdate, &wgDb)

	// start concurrent workers to download the file parts
	var wgDownload sync.WaitGroup
	for w := 1; w <= st.opts.NumThreads; w++ {
		wgDownload.Add(1)
		go st.worker(w, jobsWithUrls, jobsDbUpdate, &wgDownload)
	}

	var wgProgressReport sync.WaitGroup
	wgProgressReport.Add(1)
	st.InitDownloadStatus()
	go st.downloadProgressContinuous(&wgProgressReport)

	// wait for downloads to complete
	wgDownload.Wait()
	close(jobsDbUpdate)

	// wait for database updates to complete
	wgDb.Wait()

	// wait for progress report thread
	wgProgressReport.Wait()

	// completed all downloads
	PrintLogAndOut(st.DownloadProgressOneTime(60*1000*1000*1000) + "\n")
	PrintLogAndOut("Download completed successfully.\n")
	PrintLogAndOut("To perform additional post-download integrity checks, please use the 'inspect' subcommand.\n")
}

// UpdateDBPart.
func (st *State) updateDBPart(txn *sql.Tx, p DBPart, tsNanoSec int64) {
	switch p.(type) {
	case DBPartRegular:
		reg := p.(DBPartRegular)
		_, err := txn.Exec(fmt.Sprintf(
			"UPDATE manifest_regular_stats SET bytes_fetched = %d, download_done_time = %d WHERE file_id = '%s' AND part_id = '%d'",
			reg.Size, tsNanoSec, reg.FileId, reg.PartId))
		check(err)

	case DBPartSymlink:
		slnk := p.(DBPartSymlink)
		_, err := txn.Exec(fmt.Sprintf(
			"UPDATE manifest_symlink_stats SET bytes_fetched = %d, download_done_time = %d WHERE file_id = '%s' AND part_id = '%d'",
			slnk.Size, tsNanoSec, slnk.FileId, slnk.PartId))
		check(err)
	}
}

// There was an error in downloading a part of a file. Reset it in the
// database, so we will download it again.
func (st *State) resetDBPart(p DBPartRegular) {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	tx, err := st.db.Begin()
	check(err)
	defer tx.Commit()

	_, err = tx.Exec(fmt.Sprintf(
		"UPDATE manifest_regular_stats SET bytes_fetched = 0, download_done_time = 0 WHERE file_id = '%s' AND part_id = '%d'",
		p.FileId, p.PartId))
	check(err)
}

// Remove the local file, and zero out all the parts in the
// database.
func (st *State) resetRegularFile(p DBPartRegular) {
	// zero out the file
	folder := path.Join("./", p.Folder)
	fname := path.Join(folder, p.FileName)
	err := os.Truncate(fname, 0)
	check(err)

	st.mutex.Lock()
	defer st.mutex.Unlock()

	tx, err := st.db.Begin()
	check(err)
	defer tx.Commit()

	_, err = tx.Exec(fmt.Sprintf(
		"UPDATE manifest_regular_stats SET bytes_fetched = 0, download_done_time = 0 WHERE file_id = '%s'",
		p.FileId))
	check(err)
}

// Remove the local file, and zero out all the parts in the
// database.
func (st *State) resetSymlinkFile(slnk DXFileSymlink) {
	// zero out the file
	folder := path.Join("./", slnk.Folder)
	fname := path.Join(folder, slnk.Name)
	err := os.Truncate(fname, 0)
	check(err)

	st.mutex.Lock()
	defer st.mutex.Unlock()

	tx, err := st.db.Begin()
	check(err)
	defer tx.Commit()

	_, err = tx.Exec(fmt.Sprintf(
		"UPDATE manifest_symlink_stats SET bytes_fetched = 0, download_done_time = 0 WHERE file_id = '%s'",
		slnk.Id))
	check(err)
}

// -----------------------------------------
// inspect: validation of downloaded parts

// check that a database part has the correct md5 checksum
func (st *State) checkDBPartRegular(p DBPartRegular, integrityMsgs chan string) {
	fname := fmt.Sprintf(".%s/%s", p.Folder, p.FileName)
	if _, err := os.Stat(fname); os.IsNotExist(err) {
		st.resetRegularFile(p)
		msg := fmt.Sprintf(
			"File %s does not exist. Please re-issue the download command to resolve.",
			fname)
		integrityMsgs <- msg
		return
	}

	localf, err := os.Open(fname)
	check(err)
	defer localf.Close()

	// limit the file-descriptor to read only this part. Start at the beginning
	// of the part, and read [part-size] bytes.
	if _, err := localf.Seek(p.Offset, 0); err != nil {
		integrityMsgs <- fmt.Sprintf("Error seeking %s to %d %s", fname, p.Offset, err.Error())
		return
	}
	partReader := io.LimitReader(localf, int64(p.Size))

	hasher := md5.New()
	if _, err := io.Copy(hasher, partReader); err != nil {
		st.resetDBPart(p)
		integrityMsgs <- fmt.Sprintf("Error reading %s %s", fname, err.Error())
		return
	}
	diskSum := hex.EncodeToString(hasher.Sum(nil))

	if diskSum != p.MD5 {
		st.resetDBPart(p)
		msg := fmt.Sprintf(
			"Identified md5sum mismatch for %s part %d. Please re-issue the download command to resolve.",
			p.FileName, p.PartId)
		integrityMsgs <- msg
	}
}

func (st *State) filePartIntegrityWorker(id int, jobs <-chan JobInfo, integrityMsgs chan string, wg *sync.WaitGroup) {
	for j := range jobs {
		switch j.part.(type) {
		case DBPartRegular:
			p := j.part.(DBPartRegular)
			st.checkDBPartRegular(p, integrityMsgs)
		default:
			panic(fmt.Sprintf("bad file kind %v", j.part))
		}
	}
	wg.Done()
}

func (st *State) validateSymlinkChecksum(f DXFileSymlink, integrityMsgs chan string) {
	fname := fmt.Sprintf(".%s/%s", f.Folder, f.Name)
	if _, err := os.Stat(fname); os.IsNotExist(err) {
		st.resetSymlinkFile(f)
		fmt.Printf("File %s does not exist. Please re-issue the download command to resolve.", fname)
		return
	}

	if st.opts.Verbose {
		fmt.Printf("validate symlink %s\n", f.Name)
	}

	localf, err := os.Open(fname)
	check(err)
	defer localf.Close()

	// This is supposed to NOT load the entire file into memory.
	hasher := md5.New()
	if _, err := io.Copy(hasher, localf); err != nil {
		st.resetSymlinkFile(f)
		integrityMsgs <- fmt.Sprintf("Error reading %s %s", fname, err.Error())
		return
	}
	diskSum := hex.EncodeToString(hasher.Sum(nil))

	if diskSum == f.MD5 {
		if st.opts.Verbose {
			fmt.Printf("symlink file %s, has the correct checksum %s\n",
				f.Name, f.MD5)
		}
	} else {
		msg := fmt.Sprintf(`
Identified md5sum mismatch for symbolic link
    name      %s
    id   %s
    checksum  %s
Please re-issue the download command to resolve`,
			f.Name, f.Id, f.MD5)
		integrityMsgs <- msg
		st.resetSymlinkFile(f)
	}
}

// make sure all the part checksums are correct on disk.
func (st *State) checkAllRegularFileIntegrity() bool {
	cnt := st.queryDBIntegerResult("SELECT COUNT(*) FROM manifest_regular_stats WHERE bytes_fetched == size")
	if cnt == 0 {
		fmt.Printf("%d regular file parts to check\n", cnt)
		return true
	}
	jobs := make(chan JobInfo, cnt)
	integrityMsgs := make(chan string, cnt)
	var wg sync.WaitGroup

	rows, err := st.db.Query("SELECT * FROM manifest_regular_stats WHERE bytes_fetched == size")
	check(err)

	for rows.Next() {
		var p DBPartRegular
		err = rows.Scan(&p.FileId, &p.Project, &p.FileName, &p.Folder, &p.PartId, &p.Offset,
			&p.Size, &p.MD5, &p.BytesFetched, &p.DownloadDoneTime)
		check(err)
		j := JobInfo{
			part: p,
		}
		jobs <- j
	}
	rows.Close()
	close(jobs)

	for w := 1; w <= st.opts.NumThreads; w++ {
		wg.Add(1)
		go st.filePartIntegrityWorker(w, jobs, integrityMsgs, &wg)
	}
	wg.Wait()
	close(integrityMsgs)

	// read all the integrity messages
	numIntegrityErrors := 0
	for msg := range integrityMsgs {
		fmt.Println(msg)
		numIntegrityErrors++
	}
	if numIntegrityErrors > 0 {
		// there were errors, validation failed
		return false
	}

	fmt.Println("")
	fmt.Println("Integrity check for regular files complete.")
	return true
}

func (st *State) fileCheckSymlinkWorker(id int, jobs <-chan DXFileSymlink, integrityMsgs chan string, wg *sync.WaitGroup) {
	for j := range jobs {
		// 1. calculate the MD5 checksum of the entire file.
		// 2. compare it to the expected result
		st.validateSymlinkChecksum(j, integrityMsgs)
	}
	wg.Done()
}

func (st *State) checkAllSymlinkIntegrity() bool {
	rows, err := st.db.Query("SELECT * FROM symlinks")
	check(err)

	var allSymlinks []DXFileSymlink
	for rows.Next() {
		var f DXFileSymlink
		err := rows.Scan(&f.Folder, &f.Id, &f.ProjId, &f.Name, &f.Size, &f.MD5)
		check(err)
		allSymlinks = append(allSymlinks, f)
	}
	rows.Close()
	if len(allSymlinks) == 0 {
		return true
	}

	// skip files that weren't entirely downloaded
	var completed []DXFileSymlink
	for _, slnk := range allSymlinks {
		numBytesComplete := st.queryDBIntegerResult(
			fmt.Sprintf("SELECT SUM(bytes_fetched) FROM manifest_symlink_stats WHERE file_id = '%s'",
				slnk.Id))
		if numBytesComplete < slnk.Size {
			continue
		}
		completed = append(completed, slnk)
	}
	numSymlinksCompleted := len(completed)
	fmt.Printf("%d symlinks %d have completed downloading\n",
		len(allSymlinks), numSymlinksCompleted)
	if numSymlinksCompleted == 0 {
		return true
	}

	jobs := make(chan DXFileSymlink, numSymlinksCompleted)
	integrityMsgs := make(chan string, numSymlinksCompleted)
	var wg sync.WaitGroup

	// Create a job to verify all of the symlinks.
	//
	for _, slnk := range completed {
		if st.opts.Verbose {
			fmt.Printf("Checking symlink %s\n", slnk.Id)
		}
		jobs <- slnk
	}
	close(jobs)

	// start checking threads
	for w := 1; w <= st.opts.NumThreads; w++ {
		wg.Add(1)
		go st.fileCheckSymlinkWorker(w, jobs, integrityMsgs, &wg)
	}
	wg.Wait()
	close(integrityMsgs)

	// read all the integrity messages
	numIntegrityErrors := 0
	for msg := range integrityMsgs {
		fmt.Println(msg)
		numIntegrityErrors++
	}
	if numIntegrityErrors > 0 {
		// there were errors, validation failed
		return false
	}
	fmt.Println("")
	fmt.Println("Integrity check for symlinks complete.")
	return true
}

// check the on-disk integrity of all files
// return false if there is an integrity problem.
func (st *State) CheckFileIntegrity() bool {
	// regular files
	if !st.checkAllRegularFileIntegrity() {
		return false
	}

	// symlinks
	if !st.checkAllSymlinkIntegrity() {
		return false
	}

	return true
}
