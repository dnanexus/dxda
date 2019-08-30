package dxda

// Some inspiration + code snippets taken from https://github.com/dnanexus/precision-fda/blob/master/go/pfda.go

// TODO: Some more code cleanup + consistency with best Go practices, add more unit tests, setup deeper integration tests, add build notes
import (
	"bytes"
	"compress/bzip2"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"database/sql"

	_ "github.com/mattn/go-sqlite3"         // Following canonical example on go-sqlite3 'simple.go'
)

// A subset of the configuration parameters that the dx-toolkit uses.
//
type DXEnvironment struct {
	ApiServerHost      string
	ApiServerPort      int
	ApiServerProtocol  string
	Token              string
	DxJobId            string
}

type State struct {
	dxEnv            DXEnvironment
	opts             Opts
	mutex           *sync.Mutex
	db              *sql.DB
	ds              *DownloadStatus
	timeOfLastError  int
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func urlFailure(requestType string, url string, status string) {
	fmt.Println("ERROR when attempting API call.  Please see <manifest-file-name>.download.log for more details.")
	log.Fatalln(fmt.Errorf("%s request to '%s' failed with status %s", requestType, url, status))
}

// PrintLogAndOut ...
func PrintLogAndOut(str string) {
	fmt.Printf(str)
	log.Printf(str)
}


// Utilities to interact with the DNAnexus API
// TODO: Create automatic API wrappers for the dx toolkit
// e.g. via: https://github.com/dnanexus/dx-toolkit/tree/master/src/api_wrappers

// Opts ...
type Opts struct {
	NumThreads int // # of workers to process downloads
}

// DXConfig - Basic variables regarding DNAnexus environment config
type DXConfig struct {
	DXSECURITYCONTEXT    string `json:"DX_SECURITY_CONTEXT"`
	DXAPISERVERHOST      string `json:"DX_APISERVER_HOST"`
	DXPROJECTCONTEXTNAME string `json:"DX_PROJECT_CONTEXT_NAME"`
	DXPROJECTCONTEXTID   string `json:"DX_PROJECT_CONTEXT_ID"`
	DXAPISERVERPORT      string `json:"DX_APISERVER_PORT"`
	DXUSERNAME           string `json:"DX_USERNAME"`
	DXAPISERVERPROTOCOL  string `json:"DX_APISERVER_PROTOCOL"`
	DXCLIWD              string `json:"DX_CLI_WD"`
}

// DXAuthorization - Basic variables regarding DNAnexus authorization
type DXAuthorization struct {
	AuthToken     string `json:"auth_token"`
	AuthTokenType string `json:"auth_token_type"`
}

func safeString2Int(s string) (int) {
	i, err := strconv.Atoi(s)
	check(err)
	return i
}

/*
   Construct the environment structure. Return an additional string describing
   the source of the security token.

   The DXEnvironment has its fields set from the following sources, in order (with
   later items overriding earlier items):

   1. Hardcoded defaults
   2. Environment variables of the format DX_*
   3. Configuration file ~/.dnanexus_config/environment.json

   If no token can be obtained from these methods, an empty environment is returned.
   If the token was received from the 'DX_API_TOKEN' environment variable, the second variable in the pair
   will be the string 'environment'. If it is obtained from a DNAnexus configuration file, the second variable
   in the pair will be '.dnanexus_config/environment.json'.
*/
func GetDxEnvironment() (DXEnvironment, string, error) {
	obtainedBy := ""

	// start with hardcoded defaults
	crntDxEnv := DXEnvironment{ "api.dnanexus.com", 443, "https", "", "" }

	// override by environment variables, if they are set
	apiServerHost := os.Getenv("DX_APISERVER_HOST")
	if apiServerHost != "" {
		crntDxEnv.ApiServerHost = apiServerHost
	}
	apiServerPort := os.Getenv("DX_APISERVER_PORT")
	if apiServerPort != "" {
		crntDxEnv.ApiServerPort = safeString2Int(apiServerPort)
	}
	apiServerProtocol := os.Getenv("DX_APISERVER_PROTOCOL")
	if apiServerProtocol != "" {
		crntDxEnv.ApiServerProtocol = apiServerProtocol
	}
	securityContext := os.Getenv("DX_SECURITY_CONTEXT")
	if securityContext != "" {
		// parse the JSON format security content
		var dxauth DXAuthorization
		json.Unmarshal([]byte(securityContext), &dxauth)
		crntDxEnv.Token = dxauth.AuthToken
		obtainedBy = "environment"
	}
	envToken := os.Getenv("DX_API_TOKEN")
	if envToken != "" {
		crntDxEnv.Token = envToken
		obtainedBy = "environment"
	}
	dxJobId := os.Getenv("DX_JOB_ID")
	if dxJobId != "" {
		crntDxEnv.DxJobId = dxJobId
	}

	// Now try the configuration file
	envFile := fmt.Sprintf("%s/.dnanexus_config/environment.json", os.Getenv("HOME"))
	if _, err := os.Stat(envFile); err == nil {
		config, _ := ioutil.ReadFile(envFile)
		var dxconf DXConfig
		json.Unmarshal(config, &dxconf)
		var dxauth DXAuthorization
		json.Unmarshal([]byte(dxconf.DXSECURITYCONTEXT), &dxauth)

		crntDxEnv.ApiServerHost = dxconf.DXAPISERVERHOST
		crntDxEnv.ApiServerPort = safeString2Int(dxconf.DXAPISERVERPORT)
		crntDxEnv.ApiServerProtocol = dxconf.DXAPISERVERPROTOCOL
		crntDxEnv.Token = dxauth.AuthToken

		obtainedBy = "~/.dnanexus_config/environment.json"
	}

	// sanity checks
	var err error = nil
	if crntDxEnv.Token == "" {
		err = errors.New("could not retrieve a security token")
	}
	return crntDxEnv, obtainedBy, err
}

// Min ...
// https://mrekucci.blogspot.com/2015/07/dont-abuse-mathmax-mathmin.html
func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// TODO: ValidateManifest(manifest) + Tests

// Manifest - core type of manifest file
type Manifest map[string][]DXFile

// DXFile ...
type DXFile struct {
	Folder string            `json:"folder"`
	ID     string            `json:"id"`
	Name   string            `json:"name"`
	Parts  map[string]DXPart `json:"parts"`
}

// DXPart ...
type DXPart struct {
	MD5  string `json:"md5"`
	Size int    `json:"size"`
}

// Initialize the state
func Init(dxEnv DXEnvironment, fname string, opts Opts) *State {
	statsFname := fname + ".stats.db?cache=shared&mode=rwc"
	db, err := sql.Open("sqlite3", statsFname)
	check(err)

	return &State {
		dxEnv : dxEnv,
		opts : opts,
		mutex : &sync.Mutex{},
		db : db,
		ds : nil,
		timeOfLastError : 0,
	}
}

func (st *State) Close() {
	st.db.Close()
}

// Probably a better way to do this :)
func (st *State) queryDBIntegerResult(query string) int {
	rows, err := st.db.Query(query)
	check(err)

	var cnt int
	rows.Next()
	rows.Scan(&cnt)
	rows.Close()

	return cnt
}

// read the manifest from a file into a memory structure
func readManifest(fname string) Manifest {
	bzdata, err := ioutil.ReadFile(fname)
	check(err)
	br := bzip2.NewReader(bytes.NewReader(bzdata))
	data, err := ioutil.ReadAll(br)
	check(err)
	var m Manifest
	json.Unmarshal(data, &m)
	return m
}

// DiskSpaceString ...
// write the number of bytes as a human readable string
func diskSpaceString(numBytes uint64) string {
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
	totalSizeBytes := uint64(st.queryDBIntegerResult(
		"SELECT SUM(size) FROM manifest_stats WHERE bytes_fetched != size"))

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
	availableBytes := stat.Bavail * uint64(stat.Bsize)
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

// DXDownloadURL ...
type DXDownloadURL struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

func md5str(body []byte) string {
	hasher := md5.New()
	hasher.Write(body)
	return hex.EncodeToString(hasher.Sum(nil))
}

// DBPart ...
type DBPart struct {
	FileID           string
	Project          string
	FileName         string
	Folder           string
	PartID           int
	MD5              string
	Size             int
	BlockSize        int
	BytesFetched     int
	DownloadDoneTime int64 // The time when it completed downloading
}

// CreateManifestDB ...
func CreateManifestDB(fname string) {
	m := readManifest(fname)
	statsFname := fname + ".stats.db?cache=shared&mode=rwc"
	os.Remove(statsFname)
	db, err := sql.Open("sqlite3", statsFname)
	check(err)
	defer db.Close()

	sqlStmt := `
	CREATE TABLE manifest_stats (
		file_id text,
		project text,
		name text,
		folder text,
		part_id integer,
		md5 text,
		size integer,
		block_size integer,
		bytes_fetched integer,
                download_done_time integer
	);
	`
	_, err = db.Exec(sqlStmt)
	check(err)

	txn, err := db.Begin()
	check(err)
	for proj, files := range m {
		for _, f := range files {
			for pID := range f.Parts {
				sqlStmt = fmt.Sprintf(`
				INSERT INTO manifest_stats
				VALUES ('%s', '%s', '%s', '%s', %s, '%s', '%d', '%d', '%d', '%d');
				`,
					f.ID, proj, f.Name, f.Folder, pID, f.Parts[pID].MD5, f.Parts[pID].Size, f.Parts["1"].Size, 0, 0)
				_, err = txn.Exec(sqlStmt)
				check(err)
			}
		}
	}
	err = txn.Commit()
	check(err)
}

// PrepareFilesForDownload ...
// TODO: Optimize this for only files that need to be downloaded
//
// OQ: The 'urls' map is empty
func (st *State) prepareFilesForDownload(m Manifest) map[string]DXDownloadURL {
	urls := make(map[string]DXDownloadURL)
	for _, files := range m {
		for _, f := range files {

			// Create directory structure and initialize file if it doesn't exist
			folder := path.Join("./", f.Folder)
			fname := path.Join(folder, f.Name)
			if _, err := os.Stat(fname); os.IsNotExist(err) {
				err := os.MkdirAll(folder, 0777)
				check(err)
				localf, err := os.Create(fname)
				check(err)
				localf.Close()
			}
		}
	}
	return urls
}

// JobInfo ...
type JobInfo struct {
	part             DBPart
	wg               *sync.WaitGroup
	urls             map[string]DXDownloadURL
	tmpid            int
}

func b2MB(bytes int) int { return bytes / (1024 * 1024) }

// DownloadStatus ...
type DownloadStatus struct {
	NumParts         int
	NumBytes         int
	NumPartsComplete int
	NumBytesComplete int

	// periodicity of progress report
	ProgressInterval time.Duration

	// Size of window in nanoseconds where to look for
	// completed downloads
	MaxWindowSize int64
}

// InitDownloadStatus ...
func (st *State) InitDownloadStatus() {
	// total amounts to download, calculated once
	numParts := st.queryDBIntegerResult("SELECT COUNT(*) FROM manifest_stats")
	numBytes := st.queryDBIntegerResult("SELECT SUM(size) FROM manifest_stats")

	st.ds = &DownloadStatus{
		NumParts : numParts,
		NumBytes : numBytes,
		NumPartsComplete : 0,
		NumBytesComplete : 0,
		ProgressInterval : time.Duration(5) * time.Second,
		MaxWindowSize : int64(2 * 60 * 1000 * 1000 * 1000),
	}
}

// Calculate bandwidth in MB/sec. Query the database, and find
// all recently completed downloaded chunks.
func (st *State) calcBandwidth(timeWindowNanoSec int64) float64 {
	if timeWindowNanoSec < 1000 {
		// sanity check; if the interval is very short, just return zero.
		return 0.0
	}

	// Time and time window measured in nano seconds
	now := time.Now().UnixNano()
	lowerBound := now - timeWindowNanoSec

	query := fmt.Sprintf(
		"SELECT SUM(bytes_fetched) FROM manifest_stats WHERE download_done_time > %d",
		lowerBound)
	bytesDownloadedInTimeWindow := st.queryDBIntegerResult(query)

	// convert to megabytes downloaded divided by seconds
	mbDownloaded := float64(bytesDownloadedInTimeWindow) / float64(1024*1024)
	timeDeltaSec := float64(timeWindowNanoSec) / float64(1000*1000*1000)
	return mbDownloaded / timeDeltaSec
}

// DownloadProgressOneTime ...
// Report on progress so far
func (st *State) DownloadProgressOneTime(timeWindowNanoSec int64) string {
	// query the current progrAddess
	st.ds.NumBytesComplete = st.queryDBIntegerResult(
		"SELECT SUM(bytes_fetched) FROM manifest_stats WHERE bytes_fetched = size")
	st.ds.NumPartsComplete = st.queryDBIntegerResult(
		"SELECT COUNT(*) FROM manifest_stats WHERE bytes_fetched = size")

	// calculate bandwitdh
	bandwidthMBSec := st.calcBandwidth(timeWindowNanoSec)

	desc := fmt.Sprintf("Downloaded %d/%d MB\t%d/%d Parts (~%.1f MB/s written to disk estimated over the last %ds)",
		b2MB(st.ds.NumBytesComplete), b2MB(st.ds.NumBytes),
		st.ds.NumPartsComplete, st.ds.NumParts,
		bandwidthMBSec,
		timeWindowNanoSec/1e9)

	return desc
}

// A loop that reports on download progress periodically.
func (st *State) downloadProgressContinuous() {
	// Start time of the measurements, in nano seconds
	startTime := time.Now()

	for st.ds.NumPartsComplete < st.ds.NumParts {
		// Sleep for a number of seconds, so as to not flood the screen
		// with messages. This also substantially limits the number
		// of database queries.
		time.Sleep(st.ds.ProgressInterval)

		// If we just started the measurements, we have a short
		// history to examine. Limit the window size accordingly.
		now := time.Now()
		deltaNanoSec := now.UnixNano() - startTime.UnixNano()
		if deltaNanoSec > st.ds.MaxWindowSize {
			deltaNanoSec = st.ds.MaxWindowSize
		}
		desc := st.DownloadProgressOneTime(deltaNanoSec)
		fmt.Printf(desc)
	}
}

func (st *State) worker(id int, jobs <-chan JobInfo, wg *sync.WaitGroup) {
	const secondsInYear int = 60 * 60 * 24 * 365
	for j := range jobs {
		st.mutex.Lock()
		if _, ok := j.urls[j.part.FileID]; !ok {
			payload := fmt.Sprintf("{\"project\": \"%s\", \"duration\": %d}",
				j.part.Project, secondsInYear)

			// TODO: 100 retries
			body, err := DxAPI(&st.dxEnv, fmt.Sprintf("%s/download", j.part.FileID), payload)
			check(err)
			var u DXDownloadURL
			json.Unmarshal(body, &u)
			j.urls[j.part.FileID] = u
		}
		st.mutex.Unlock()

		// TODO: 25 retries
		st.downloadDBPart(j.part, j.wg, j.urls)
	}
	wg.Done()
}

func (st *State) fileIntegrityWorker(id int, jobs <-chan JobInfo, wg *sync.WaitGroup) {
	for j := range jobs {
		st.checkDBPart(j.part, j.wg)

		if st.dxEnv.DxJobId == "" {
			// running on a console, erase the previous line
			// TODO: Get rid of this temporary space-padding fix for carriage returns
			fmt.Printf("                                                                      \r")
			fmt.Printf("%s:%d\r", j.part.FileName, j.part.PartID)
		} else {
			// We are on a dx-job, and we want to see the history of printouts
			fmt.Printf("%s:%d\n")
		}
	}
	wg.Done()
}

// Download all the files that are mentioned in the manifest.
func (st *State) DownloadManifestDB(fname string) {
	st.timeOfLastError = time.Now().Second()
	// TODO: Update to not require manifest structure read into memory
	m := readManifest(fname)

	// TODO Log network settings and other helpful info for debugging

	PrintLogAndOut("Preparing files for download\n")
	urls := st.prepareFilesForDownload(m)

	// Limit the number of threads
	runtime.GOMAXPROCS(st.opts.NumThreads)
	PrintLogAndOut(fmt.Sprintf("Downloading files using %d threads\n", st.opts.NumThreads))

	cnt := st.queryDBIntegerResult("SELECT COUNT(*) FROM manifest_stats WHERE bytes_fetched != size")
	rows, err := st.db.Query("SELECT * FROM manifest_stats WHERE bytes_fetched != size")
	check(err)

	jobs := make(chan JobInfo, cnt)
	var wg sync.WaitGroup

	for i := 1; rows.Next(); i++ {
		var p DBPart
		err = rows.Scan(&p.FileID, &p.Project, &p.FileName, &p.Folder, &p.PartID, &p.MD5, &p.Size,
			&p.BlockSize, &p.BytesFetched, &p.DownloadDoneTime)
		check(err)
		var j JobInfo
		j.part = p
		j.wg = &wg
		j.urls = urls
		j.tmpid = i
		jobs <- j
	}
	close(jobs)
	rows.Close()

	for w := 1; w <= st.opts.NumThreads; w++ {
		wg.Add(1)
		go st.worker(w, jobs, &wg)
	}

	st.InitDownloadStatus()

	//go downloadProgressContinuous(&ds)
	wg.Wait()
	PrintLogAndOut(st.DownloadProgressOneTime(60*1000*1000*1000) + "\n")
	PrintLogAndOut("Download completed successfully.\n")
	PrintLogAndOut("To perform additional post-download integrity checks, please use the 'inspect' subcommand.\n")

}

// CheckFileIntegrity ...
func (st *State) CheckFileIntegrity() {
	cnt := st.queryDBIntegerResult("SELECT COUNT(*) FROM manifest_stats WHERE bytes_fetched == size")
	rows, err := st.db.Query("SELECT * FROM manifest_stats WHERE bytes_fetched == size")

	jobs := make(chan JobInfo, cnt)

	var wg sync.WaitGroup

	for i := 1; rows.Next(); i++ {
		var p DBPart
		err = rows.Scan(&p.FileID, &p.Project, &p.FileName, &p.Folder, &p.PartID, &p.MD5, &p.Size,
			&p.BlockSize, &p.BytesFetched, &p.DownloadDoneTime)
		check(err)
		var j JobInfo
		j.part = p
		j.wg = &wg
		j.tmpid = i
		jobs <- j
	}
	close(jobs)
	rows.Close()

	for w := 1; w <= st.opts.NumThreads; w++ {
		wg.Add(1)
		go st.fileIntegrityWorker(w, jobs, &wg)
	}
	wg.Wait()
	fmt.Println("")
	fmt.Println("Integrity check complete.")
}

// UpdateDBPart. Locking is done by the database.
func (st *State) updateDBPart(p DBPart) {
	tx, err := st.db.Begin()
	check(err)
	defer tx.Commit()

	now := time.Now().UnixNano()
	_, err = tx.Exec(fmt.Sprintf(
		"UPDATE manifest_stats SET bytes_fetched = %d, download_done_time = %d WHERE file_id = '%s' AND part_id = '%d'",
		p.Size, now, p.FileID, p.PartID))
	check(err)

}

// ResetDBPart ...
func (st *State) resetDBPart(p DBPart) {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	tx, err := st.db.Begin()
	check(err)
	defer tx.Commit()

	_, err = tx.Exec(fmt.Sprintf(
		"UPDATE manifest_stats SET bytes_fetched = 0, download_done_time = 0 WHERE file_id = '%s' AND part_id = '%d'",
		p.FileID, p.PartID))
	check(err)
}

// ResetDBFile ...
func (st *State) resetDBFile(p DBPart) {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	tx, err := st.db.Begin()
	check(err)
	defer tx.Commit()

	_, err = tx.Exec(fmt.Sprintf(
		"UPDATE manifest_stats SET bytes_fetched = 0, download_done_time = 0 WHERE file_id = '%s'",
		p.FileID))
	check(err)
}

// Download part of a file
func (st *State) downloadDBPart(
	p DBPart,
	wg *sync.WaitGroup,
	urls map[string]DXDownloadURL) error {

	timer := time.AfterFunc(10*time.Minute, func() {
		panic("Timeout for downloading part exceeded.")
	})
	defer timer.Stop()
	fname := fmt.Sprintf(".%s/%s", p.Folder, p.FileName)
	localf, err := os.OpenFile(fname, os.O_WRONLY, 0777)
	check(err)
	headers := make(map[string]string)
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", (p.PartID-1)*p.BlockSize, p.PartID*p.BlockSize-1)

	// TODO: Avoid locking here?
	st.mutex.Lock()
	u := urls[p.FileID]
	st.mutex.Unlock()

	for k, v := range u.Headers {
		headers[k] = v
	}
	body, err := dxHttpRequestChecksum("GET", u.URL, headers, []byte("{}"), &p)
	check(err)
	_, err = localf.Seek(int64((p.PartID-1)*p.BlockSize), 0)
	check(err)
	_, err = localf.Write(body)
	check(err)
	localf.Close()

	st.updateDBPart(p)
	progressStr := st.DownloadProgressOneTime(60*1000*1000*1000)

	if st.dxEnv.DxJobId == "" {
		// running on a console, erase the previous line
		// TODO: Get rid of this temporary space-padding fix for carriage returns
		fmt.Printf("                                                                      \r")
		fmt.Printf(progressStr + "\r")
	} else {
		// running on a job, we want to see the history
		fmt.Printf(progressStr + "\n")
	}
	log.Printf(progressStr + "\n")
	return nil
}

// Add retries around the core http-request method
//
func dxHttpRequestChecksum(
	requestType string,
	url string,
	headers map[string]string,
	data []byte,
	p *DBPart) (body []byte, err error) {
	tCnt := 0
	for tCnt < 3 {
		body, err := DxHttpRequest(requestType, url, headers, data)
		if err != nil {
			return nil, err
		}

		// check that the length is correct
		recvLen := len(body)
		if recvLen != p.Size {
			log.Printf("received length is wrong, got %d, expected %d. Retrying.", recvLen, p.Size)
			tCnt++
			continue
		}

		// Verify the checksum.
		recvChksum := md5str(body)
		if recvChksum == p.MD5 {
			// good checksum
			return body, nil
		}

		// bad checksum, we need to retry
		log.Printf("MD5 string of part ID %d does not match stored MD5sum. Retrying.", p.PartID)
		tCnt++
	}

	err = fmt.Errorf("%s request to '%s' failed after %d attempts",
		requestType, url, tCnt)
	return nil, err
}

// check that a database part has the correct md5 checksum
func (st *State) checkDBPart(p DBPart, wg *sync.WaitGroup) {
	fname := fmt.Sprintf(".%s/%s", p.Folder, p.FileName)
	if _, err := os.Stat(fname); os.IsNotExist(err) {
		st.resetDBFile(p)
		fmt.Printf("File %s does not exist. Please re-issue the download command to resolve.", fname)
	} else {
		localf, err := os.Open(fname)
		check(err)
		_, err = localf.Seek(int64((p.PartID-1)*p.BlockSize), 0)
		check(err)
		body := make([]byte, p.Size)
		_, err = localf.Read(body)
		check(err)
		localf.Close()

		if md5str(body) != p.MD5 {
			fmt.Printf("Identified md5sum mismatch for %s part %d. Please re-issue the download command to resolve.\n", p.FileName, p.PartID)
			st.resetDBPart(p)
		}
	}
}
