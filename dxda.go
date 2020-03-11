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
	"log"
	"io"
	"os"
	"path"
	"runtime"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"         // Following canonical example on go-sqlite3 'simple.go'
	"github.com/hashicorp/go-retryablehttp" // http client library
)

const (
	// Range for the number of threads we want to use
	minNumThreads = 2
	maxNumThreads = 32

	numRetries = 10
	numRetriesChecksumMismatch = 3
	secondsInYear int = 60 * 60 * 24 * 365

	// Constraints:
	// 1) The chunk should be large enough to optimize the http(s) network stack
	// 2) The chunk should be small enough to be able to take it in one request,
	//    causing minimal retries.
	maxChunkSize = 16 * MiB
)

// DXDownloadURL ...
type DXDownloadURL struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

// a part to be downloaded. Can be:
// 1) part of a regular file
// 2) part of symbolic link (a web address)
type DBPart interface {
	folder()   string
	fileName() string
	offset()   int64
	size()     int
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
func (slnk DBPartSymlink) offset() int64    { return slnk.Offset }
func (slnk DBPartSymlink) size() int        { return slnk.Size }


// JobInfo ...
type JobInfo struct {
	part             DBPart
	wg               *sync.WaitGroup
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
	dxEnv            DXEnvironment
	opts             Opts
	mutex            sync.Mutex
	db              *sql.DB
	ds              *DownloadStatus
	urls             map[string]DXDownloadURL
	timeOfLastError  int
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
func calcNumThreads() int {
	numCPUs := runtime.NumCPU()
	hwMemoryBytes := memorySizeBytes()

	fmt.Printf("number of machine cores: %d\n", numCPUs)
	fmt.Printf("memory size: %d GiB\n", hwMemoryBytes/GiB)

	numThreads := MinInt(2 * numCPUs, maxNumThreads)
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

	// if the number of threads isn't set we
	// need to calculate it and modify the options
	opts := optsRaw
	if opts.NumThreads == 0 {
		opts.NumThreads = calcNumThreads()
	}

	// Limit the number of threads
	runtime.GOMAXPROCS(opts.NumThreads)
	fmt.Printf("Downloading files using %d threads\n", opts.NumThreads)

	return &State {
		dxEnv : dxEnv,
		opts : opts,
		mutex : sync.Mutex{},
		db : db,
		ds : nil,
		urls : make(map[string]DXDownloadURL),
		timeOfLastError : 0,
	}
}

func (st *State) Close() {
	st.db.Close()
}

func (st *State) printToStdout(a string, args ...interface{}) {
	line := fmt.Sprintf(a, args...)

	if st.dxEnv.DxJobId == "" {
		// running on a console, erase the previous line
		// TODO: Get rid of this temporary space-padding fix for carriage returns
		fmt.Printf("                                                                      \r")
		fmt.Printf("%s\r", line)
	} else {
		// We are on a dx-job, and we want to see the history of printouts
		fmt.Print(line)
	}
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
	totalSizeBytes := st.queryDBIntegerResult(
		"SELECT SUM(size) FROM manifest_regular_stats WHERE bytes_fetched != size")

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
	diskSpaceStr := fmt.Sprintf("Required disk space = %s, available = %s,  #free-inodes=%d\n",
		diskSpaceString(totalSizeBytes),
		diskSpaceString(availableBytes),
		stat.Ffree)
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
		endOfs := MinInt64(offset + maxChunkSize, slnk.Size)
		partLen := endOfs - offset

		if partLen <= 0 {
			panic(fmt.Sprintf("part length could not be zero or less (%d)", partLen))
		}
		sqlStmt := fmt.Sprintf(`
				INSERT INTO manifest_symlink_stats
				VALUES ('%s', '%s', '%s', '%s', '%d', '%d', '%d', '%d', '%d', '%s');
				`,
			slnk.Id, slnk.ProjId, slnk.Name, slnk.Folder, pId, offset, partLen, 0, 0, slnk.Url)
		_, err := txn.Exec(sqlStmt)
		check(err)
		offset += partLen
		pId += 1
	}

	// add to global table
	sqlStmt := fmt.Sprintf(`
		INSERT INTO symlinks
		VALUES ('%s', '%s', '%s', '%s', '%d', '%s', '%s');
		`,
		slnk.Folder, slnk.Id, slnk.ProjId, slnk.Name, slnk.Size, slnk.Url, slnk.MD5)
	_, err := txn.Exec(sqlStmt)
	check(err)
}


// Read the manifest file, and build a database with an empty state
// for each part in each file.
func (st *State) CreateManifestDB(manifest Manifest, fname string) {
	statsFname := fname + ".stats.db?_busy_timeout=60000&cache=shared&mode=rwc"
	os.Remove(statsFname)
	db, err := sql.Open("sqlite3", statsFname)
	check(err)
	defer db.Close()

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
	_, err = db.Exec(sqlStmt)
	check(err)

	// symbolic link parts do not have md5 checksums, and they
	// can use the URL directly.
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
                download_done_time integer,
                url text
	);
	`
	_, err = db.Exec(sqlStmt)
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
	        url     text,
	        md5     text
	);
	`
	_, err = db.Exec(sqlStmt)
	check(err)

	txn, err := db.Begin()
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
	numParts := st.queryDBIntegerResult("SELECT COUNT(*) FROM manifest_regular_stats")
	numBytes := st.queryDBIntegerResult("SELECT SUM(size) FROM manifest_regular_stats")

	st.ds = &DownloadStatus{
		NumParts : numParts,
		NumBytes : numBytes,
		NumPartsComplete : 0,
		NumBytesComplete : 0,
		ProgressInterval : time.Duration(5) * time.Second,
		MaxWindowSize : int64(2 * 60 * 1000 * 1000 * 1000),
	}

	if st.opts.Verbose {
		log.Printf("init dowload status %v\n", st.ds)
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
	st.ds.NumBytesComplete = st.queryDBIntegerResult(
		"SELECT SUM(bytes_fetched) FROM manifest_regular_stats WHERE bytes_fetched = size")
	st.ds.NumPartsComplete = st.queryDBIntegerResult(
		"SELECT COUNT(*) FROM manifest_regular_stats WHERE bytes_fetched = size")

	// calculate bandwitdh
	bandwidthMBSec := st.calcBandwidth(timeWindowNanoSec)

	desc := fmt.Sprintf("Downloaded %d/%d MB\t%d/%d Parts (~%.1f MB/s written to disk estimated over the last %ds)",
		bytes2MiB(st.ds.NumBytesComplete), bytes2MiB(st.ds.NumBytes),
		st.ds.NumPartsComplete, st.ds.NumParts,
		bandwidthMBSec,
		timeWindowNanoSec/1e9)

	return desc
}

// create a download url if one doesn't exist
func (st *State) createURL(p DBPartRegular, httpClient *retryablehttp.Client) DXDownloadURL {
	var u DXDownloadURL

	// check if we already have it
	st.mutex.Lock()
	u, ok := st.urls[p.FileId]
	st.mutex.Unlock()
	if ok {
		return u
	}

	// a regular DNAx file. Requires generating a pre-authenticated download URL.
	payload := fmt.Sprintf("{\"project\": \"%s\", \"duration\": %d}",
		p.Project, secondsInYear)

	body, err := DxAPI(
		context.TODO(),
		httpClient,
		numRetries,
		&st.dxEnv,
		fmt.Sprintf("%s/download", p.FileId),
		payload)
	check(err)

	if err := json.Unmarshal(body, &u); err != nil {
		log.Printf(err.Error())
		panic("Could not unmarshal response from dnanexus for download URL")
	}

	// record the pre-auth URL so we don't have to create it again
	st.mutex.Lock()
	st.urls[p.FileId] = u
	st.mutex.Unlock()

	return u
}

// Download part of a symlink
func (st *State) downloadSymlinkPart(
	httpClient *retryablehttp.Client,
	p DBPartSymlink,
	wg *sync.WaitGroup,
	u DXDownloadURL) error {

	if st.opts.Verbose {
		log.Printf("downloadSymlinkPart %v %v\n", p, u)
	}

	fname := fmt.Sprintf(".%s/%s", p.folder(), p.fileName())
	localf, err := os.OpenFile(fname, os.O_WRONLY, 0777)
	check(err)
	defer localf.Close()

	headers := make(map[string]string)
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", p.offset(), p.offset() + int64(p.size()) - 1)

	for k, v := range u.Headers {
		headers[k] = v
	}
	body, err := st.dxHttpRequestData(httpClient, "GET", u.URL, headers, []byte("{}"), p.Size)
	check(err)
	_, err = localf.WriteAt(body, p.offset())
	check(err)

	st.updateDBPart(p)
	progressStr := st.DownloadProgressOneTime(60*1000*1000*1000)

	st.printToStdout(progressStr)
	log.Printf(progressStr + "\n")
	return nil
}

// Download part of a file and verify its checksum in memory
//
func (st *State) downloadRegPartCheckSum(
	httpClient *retryablehttp.Client,
	p DBPartRegular,
	wg *sync.WaitGroup,
	u DXDownloadURL) (bool, error) {

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
	for ofs := p.Offset; ofs <= endPart; ofs += maxChunkSize {
		chunkEnd := MinInt64(ofs + maxChunkSize - 1, endPart)
		chunkSize := int(chunkEnd - ofs + 1)

		headers := make(map[string]string)
		headers["Range"] = fmt.Sprintf("bytes=%d-%d", ofs, chunkEnd)
		for k, v := range u.Headers {
			headers[k] = v
		}

		body, err := st.dxHttpRequestData(httpClient, "GET", u.URL, headers, []byte("{}"), chunkSize)
		check(err)
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

	st.updateDBPart(p)
	progressStr := st.DownloadProgressOneTime(60*1000*1000*1000)

	st.printToStdout(progressStr)
	log.Printf(progressStr + "\n")
	return true, nil
}

func (st *State) downloadRegPart(
	httpClient *retryablehttp.Client,
	p DBPartRegular,
	wg *sync.WaitGroup,
	u DXDownloadURL) error {

	for i := 0 ; i < numRetriesChecksumMismatch; i++ {
		ok, err := st.downloadRegPartCheckSum(httpClient, p, wg, u)
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

func (st *State) worker(id int, jobs <-chan JobInfo, wg *sync.WaitGroup) {
	// Create one http client per worker. This should, hopefully, allow
	// caching open TCP/HTTP connections, reducing startup times.
	httpClient := NewHttpClient(true)

	for j := range jobs {
		switch j.part.(type) {
		case DBPartRegular:
			p := j.part.(DBPartRegular)
			u := st.createURL(p, httpClient)
			st.downloadRegPart(httpClient, p, j.wg, u)

		case DBPartSymlink:
			pLnk := j.part.(DBPartSymlink)
			u := DXDownloadURL{
				URL : pLnk.Url,
				Headers : make(map[string]string, 0),
			}
			st.downloadSymlinkPart(httpClient, pLnk, j.wg, u)
		}
	}
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
	cnt_reg := st.queryDBIntegerResult("SELECT COUNT(*) FROM manifest_regular_stats WHERE bytes_fetched != size")
	cnt_slnk := st.queryDBIntegerResult("SELECT COUNT(*) FROM manifest_symlink_stats WHERE bytes_fetched != size")
	jobs := make(chan JobInfo, cnt_reg + cnt_slnk)
	var wg sync.WaitGroup

	// create a job for each incomplete data file part
	st.mutex.Lock()
	rows, err := st.db.Query("SELECT * FROM manifest_regular_stats WHERE bytes_fetched != size")
	st.mutex.Unlock()
	check(err)

	numRows := 0
	for rows.Next() {
		var p DBPartRegular
		err := rows.Scan(&p.FileId, &p.Project, &p.FileName, &p.Folder, &p.PartId, &p.Offset,
			&p.Size, &p.MD5, &p.BytesFetched, &p.DownloadDoneTime)
		check(err)
		j := JobInfo{
			part : p,
			wg : &wg,
		}
		jobs <- j
		numRows++
	}
	rows.Close()
	if st.opts.Verbose {
		log.Printf("There are %d regular file pieces\n", numRows)
	}

	// create a job for each imcomplete data symlink part
	st.mutex.Lock()
	rows, err = st.db.Query("SELECT * FROM manifest_symlink_stats WHERE bytes_fetched != size")
	st.mutex.Unlock()
	check(err)

	for rows.Next() {
		var p DBPartSymlink
		err = rows.Scan(&p.FileId, &p.Project, &p.FileName, &p.Folder, &p.PartId, &p.Offset,
			&p.Size, &p.BytesFetched, &p.DownloadDoneTime, &p.Url)
		check(err)
		j := JobInfo {
			part : p,
			wg : &wg,
		}
		jobs <- j
	}
	rows.Close()

	// Close the job channel, there will be no more jobs.
	close(jobs)

	// start concurrent workers to download the file parts
	for w := 1; w <= st.opts.NumThreads; w++ {
		wg.Add(1)
		go st.worker(w, jobs, &wg)
	}

	st.InitDownloadStatus()

	// wait for all the jobs to be completed
	wg.Wait()

	// completed all downloads
	PrintLogAndOut(st.DownloadProgressOneTime(60*1000*1000*1000) + "\n")
	PrintLogAndOut("Download completed successfully.\n")
	PrintLogAndOut("To perform additional post-download integrity checks, please use the 'inspect' subcommand.\n")
}


// UpdateDBPart.
func (st *State) updateDBPart(p DBPart) {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	tx, err := st.db.Begin()
	check(err)
	defer tx.Commit()

	now := time.Now().UnixNano()

	switch p.(type) {
	case DBPartRegular:
		reg := p.(DBPartRegular)
		_, err = tx.Exec(fmt.Sprintf(
			"UPDATE manifest_regular_stats SET bytes_fetched = %d, download_done_time = %d WHERE file_id = '%s' AND part_id = '%d'",
			reg.Size, now, reg.FileId, reg.PartId))
		check(err)

	case DBPartSymlink:
		slnk := p.(DBPartSymlink)
		_, err = tx.Exec(fmt.Sprintf(
			"UPDATE manifest_symlink_stats SET bytes_fetched = %d, download_done_time = %d WHERE file_id = '%s' AND part_id = '%d'",
			slnk.Size, now, slnk.FileId, slnk.PartId))
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


// Add retries around the core http-request method
//
func (st *State) dxHttpRequestData(
	httpClient *retryablehttp.Client,
	requestType string,
	url string,
	headers map[string]string,
	data []byte,
	dataLen int) (body []byte, err error) {
	tCnt := 0

	// Safety procedure to force timeout to prevent hanging
	ctx, cancel := context.WithCancel(context.TODO())
	timer := time.AfterFunc(requestOverallTimout, func() {
		cancel()
	})
	defer timer.Stop()

	for tCnt < 3 {
		body, err := DxHttpRequest(ctx, httpClient, numRetries, requestType, url, headers, data)
		if err != nil {
			return nil, err
		}

		// check that the length is correct
		recvLen := len(body)
		if recvLen != dataLen {
			// Note: it would be preferable to collect partial results and concatenate them.
			log.Printf("received length is wrong, got %d, expected %d. Retrying.", recvLen, dataLen)
			tCnt++
			continue
		}
		return body, nil
	}

	err = fmt.Errorf("%s request to '%s' failed after %d attempts",
		requestType, url, tCnt)
	return nil, err
}

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
		st.printToStdout("validate symlink %s", f.Name)
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
			fmt.Printf("file %s,symlink %s, has the correct checksum %s\n",
				f.Name, f.Url, f.MD5)
		}
	} else {
		msg := fmt.Sprintf(`
Identified md5sum mismatch for symbolic link
    name      %s
    symlink   %s
    checksum  %s
Please re-issue the download command to resolve`,
			f.Name, f.Url, f.MD5)
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

	st.mutex.Lock()
	rows, err := st.db.Query("SELECT * FROM manifest_regular_stats WHERE bytes_fetched == size")
	st.mutex.Unlock()
	check(err)

	for rows.Next() {
		var p DBPartRegular
		err = rows.Scan(&p.FileId, &p.Project, &p.FileName, &p.Folder, &p.PartId, &p.Offset,
			&p.Size, &p.MD5, &p.BytesFetched, &p.DownloadDoneTime)
		check(err)
		j := JobInfo{
			part : p,
			wg : &wg,
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
	st.mutex.Lock()
	rows, err := st.db.Query("SELECT * FROM symlinks")
	st.mutex.Unlock()
	check(err)

	var allSymlinks []DXFileSymlink
	for rows.Next() {
		var f DXFileSymlink
		err := rows.Scan(&f.Folder, &f.Id, &f.ProjId, &f.Name, &f.Size, &f.Url, &f.MD5)
		check(err)
		allSymlinks = append(allSymlinks, f)
	}
	rows.Close()
	if len(allSymlinks) == 0 {
		return true
	}

	// skip files that weren't entirely downloaded
	var completed []DXFileSymlink
	for _, slnk := range(allSymlinks) {
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
	for _, slnk := range(completed) {
		if st.opts.Verbose {
			fmt.Println("Checking symlink %s", slnk.Url)
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
