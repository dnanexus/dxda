package dxda

// Some inspiration + code snippets taken from https://github.com/dnanexus/precision-fda/blob/master/go/pfda.go

// TODO: Some more code cleanup + consistency with best Go practices, add more unit tests, setup deeper integration tests, add build notes
import (
	"bytes"
	"compress/bzip2"
	"crypto/md5"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"database/sql"

	"github.com/hashicorp/go-cleanhttp"     // required by go-retryablehttp
	"github.com/hashicorp/go-retryablehttp" // use http libraries from hashicorp for implement retry logic
	_ "github.com/mattn/go-sqlite3"         // Following canonical example on go-sqlite3 'simple.go'
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func urlFailure(requestType string, url string, status string) {
	log.Fatalln(fmt.Errorf("%s request to '%s' failed with status %s", requestType, url, status))
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

// GetToken - Get DNAnexus authentication token
/*
   Returns a pair of strings representing the authentication token and where it was received from
   If the environment variable DX_API_TOKEN is set, the token is obtained from it
   Otherwise, the token is obtained from the '~/.dnanexus_config/environment.json' file
   If no token can be obtained from these methods, a pair of empty strings is returned
   If the token was received from the 'DX_API_TOKEN' environment variable, the second variable in the pair
   will be the string 'environment'.  If it is obtained from a DNAnexus configuration file, the second variable
   in the pair will be '.dnanexus_config/environment.json'.
*/
func GetToken() (string, string) {
	envToken := os.Getenv("DX_API_TOKEN")
	envFile := fmt.Sprintf("%s/.dnanexus_config/environment.json", os.Getenv("HOME"))
	if envToken != "" {
		return envToken, "environment"
	}
	if _, err := os.Stat(envFile); err == nil {
		config, _ := ioutil.ReadFile(envFile)
		var dxconf DXConfig
		json.Unmarshal(config, &dxconf)
		var dxauth DXAuthorization
		json.Unmarshal([]byte(dxconf.DXSECURITYCONTEXT), &dxauth)
		return dxauth.AuthToken, "~/.dnanexus_config/environment.json"
	}
	return "", ""
}

func makeRequestWithHeadersFail(requestType string, url string, headers map[string]string, data []byte) (status string, body []byte) {
	const minRetryTime = 1   // seconds
	const maxRetryTime = 120 // seconds
	const maxRetryCount = 10
	const userAgent = "DNAnexus Download Agent (v. 0.1)"

	var client *retryablehttp.Client
	localCertFile := os.Getenv("DX_TLS_CERTIFICATE_FILE")
	if localCertFile == "" {
		client = &retryablehttp.Client{
			HTTPClient:   cleanhttp.DefaultClient(),
			Logger:       log.New(ioutil.Discard, "", 0), // Throw away retryablehttp internal logging
			RetryWaitMin: minRetryTime * time.Second,
			RetryWaitMax: maxRetryTime * time.Second,
			RetryMax:     maxRetryCount,
			CheckRetry:   retryablehttp.DefaultRetryPolicy,
		}
	} else {
		insecure := false
		if os.Getenv("DX_TLS_SKIP_VERIFY") == "true" {
			insecure = true
		}

		// Get the SystemCertPool, continue with an empty pool on error
		rootCAs, _ := x509.SystemCertPool()
		if rootCAs == nil {
			rootCAs = x509.NewCertPool()
		}

		// Read in the cert file
		certs, err := ioutil.ReadFile(localCertFile)
		check(err)

		// Append our cert to the system pool
		if ok := rootCAs.AppendCertsFromPEM(certs); !ok {
			fmt.Println("No certs appended, using system certs only")
		}

		// Trust the augmented cert pool in our client
		config := &tls.Config{
			InsecureSkipVerify: insecure,
			RootCAs:            rootCAs,
		}

		tr := cleanhttp.DefaultTransport()
		tr.TLSClientConfig = config

		client = &retryablehttp.Client{
			HTTPClient:   &http.Client{Transport: tr},
			Logger:       log.New(ioutil.Discard, "", 0), // Throw away retryablehttp internal logging
			RetryWaitMin: minRetryTime * time.Second,
			RetryWaitMax: maxRetryTime * time.Second,
			RetryMax:     maxRetryCount,
			CheckRetry:   retryablehttp.DefaultRetryPolicy,
		}
	}

	req, err := retryablehttp.NewRequest(requestType, url, bytes.NewReader(data))
	check(err)
	for header, value := range headers {
		req.Header.Set(header, value)
	}
	resp, err := client.Do(req)
	check(err)
	defer resp.Body.Close()
	status = resp.Status
	body, _ = ioutil.ReadAll(resp.Body)

	if !strings.HasPrefix(status, "2") {
		urlFailure(requestType, url, status)
	}
	return status, body
}

// DXAPI (WIP) - Function to wrap a generic API call to DNAnexus
func DXAPI(token, api string, payload string) (status string, body []byte) {
	headers := map[string]string{
		"User-Agent":    "DNAnexus Download Client v0.1",
		"Authorization": fmt.Sprintf("Bearer %s", token),
		"Content-Type":  "application/json",
	}
	apiServer := os.Getenv("DX_API_SERVER")
	if apiServer == "" {
		apiServer = "api.dnanexus.com"
	}
	url := fmt.Sprintf("https://%s/%s", apiServer, api)
	return makeRequestWithHeadersFail("POST", url, headers, []byte(payload))
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

// Probably a better way to do this :)
func queryDBIntegerResult(query, dbFname string) int {
	statsFname := dbFname + "?_busy_timeout=60000&cache=shared&mode=rc"

	db, err := sql.Open("sqlite3", statsFname)
	check(err)
	defer db.Close()

	rows, err := db.Query(query)
	check(err)
	var cnt int
	rows.Next()
	rows.Scan(&cnt)
	rows.Close()

	return cnt
}

// ReadManifest ...
func ReadManifest(fname string) Manifest {
	bzdata, err := ioutil.ReadFile(fname)
	check(err)
	br := bzip2.NewReader(bytes.NewReader(bzdata))
	data, err := ioutil.ReadAll(br)
	check(err)
	var m Manifest
	json.Unmarshal(data, &m)
	return m
}


// write the number of bytes as a human readable string
func DiskSpaceString(numBytes uint64) string {
	//KB = 1024
	const MB = 1024 * 1024
	const GB = 1024 * 1024 * 1024
	const TB = 1024 * 1024 * 1024 * 1024

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


// Check that we have enough disk space for all downloaded files
func CheckDiskSpace(fname string) error {
	// Calculate total disk space required. To get an accurate number,
	// query the database, and sum the space for missing pieces.
	//
	dbFname := fname + ".stats.db"
	totalSizeBytes := uint64(queryDBIntegerResult("SELECT SUM(size) FROM manifest_stats WHERE bytes_fetched != size",
		dbFname))

	// Find how much local disk space is available
	var stat syscall.Statfs_t
	wd, err := os.Getwd()
	check(err)
	err2 := syscall.Statfs(wd, &stat)
	check(err2)

	// Available blocks * size per block = available space in bytes
	availableBytes := stat.Bavail * uint64(stat.Bsize)
	if availableBytes < totalSizeBytes {
		desc := fmt.Sprintf("Not enough disk space, available = %s, required = %s",
			DiskSpaceString(availableBytes),
			DiskSpaceString(totalSizeBytes))
		return errors.New(desc)
	}
	fmt.Printf("Required disk space = %s, available = %s\n",
		DiskSpaceString(totalSizeBytes),
		DiskSpaceString(availableBytes))
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
	FileID       string
	Project      string
	FileName     string
	Folder       string
	PartID       int
	MD5          string
	Size         int
	BlockSize    int
	BytesFetched int
}

// CreateManifestDB ...
func CreateManifestDB(fname string) {
	m := ReadManifest(fname)
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
		bytes_fetched integer
	);
	`
	_, err = db.Exec(sqlStmt)
	check(err)

	_, err = db.Exec("BEGIN TRANSACTION")
	check(err)
	for proj, files := range m {
		for _, f := range files {
			for pID := range f.Parts {
				sqlStmt = fmt.Sprintf(`
				INSERT INTO manifest_stats
				VALUES ('%s', '%s', '%s', '%s', %s, '%s', '%d', '%d', '%d');
				`,
					f.ID, proj, f.Name, f.Folder, pID, f.Parts[pID].MD5, f.Parts[pID].Size, f.Parts["1"].Size, 0)
				_, err = db.Exec(sqlStmt)
				check(err)
			}
		}
	}
	_, err = db.Exec("END TRANSACTION")
	check(err)
}

// PrepareFilesForDownload ...
// TODO: Optimize this for only files that need to be downloaded
//
// OQ: The 'urls' map is empty
func PrepareFilesForDownload(m Manifest, token string) map[string]DXDownloadURL {
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
	manifestFileName string
	part             DBPart
	wg               *sync.WaitGroup
	urls             map[string]DXDownloadURL
	tmpid            int
}

func b2MB(bytes int) int { return bytes / 1000000 }

// DownloadProgress ...
func DownloadProgress(fname string) string {
	// TODO: memoize totals so DB is not re-queried

	dbFname := fname + ".stats.db"
	numPartsComplete := queryDBIntegerResult("SELECT COUNT(*) FROM manifest_stats WHERE bytes_fetched = size", dbFname)
	numParts := queryDBIntegerResult("SELECT COUNT(*) FROM manifest_stats", dbFname)

	numBytesComplete := queryDBIntegerResult("SELECT SUM(bytes_fetched) FROM manifest_stats WHERE bytes_fetched = size", dbFname)
	numBytes := queryDBIntegerResult("SELECT SUM(size) FROM manifest_stats", dbFname)

	return fmt.Sprintf("%d/%d MB\t%d/%d Parts Downloaded", b2MB(numBytesComplete), b2MB(numBytes), numPartsComplete, numParts)
}

const secondsInYear int = 60 * 60 * 24 * 365

func worker(id int, jobs <-chan JobInfo, token string, mutex *sync.Mutex, wg *sync.WaitGroup) {
	for j := range jobs {
		if _, ok := j.urls[j.part.FileID]; !ok {
			payload := fmt.Sprintf("{\"project\": \"%s\", \"duration\": %d}",
				j.part.Project, secondsInYear)
			_, body := DXAPI(token, fmt.Sprintf("%s/download", j.part.FileID), payload)
			var u DXDownloadURL
			json.Unmarshal(body, &u)
			mutex.Lock()
			j.urls[j.part.FileID] = u
			mutex.Unlock()
		}
		recoverer(10, DownloadDBPart, j.manifestFileName, j.part, j.wg, j.urls, mutex)
		fmt.Printf("%s\r", DownloadProgress(j.manifestFileName))
	}
	wg.Done()
}

func fileIntegrityWorker(id int, jobs <-chan JobInfo, mutex *sync.Mutex, wg *sync.WaitGroup) {
	for j := range jobs {
		CheckDBPart(j.manifestFileName, j.part, j.wg, mutex)
		fmt.Printf("%s:%d\r", j.part.FileName, j.part.PartID)
	}
	wg.Done()
}

type downloader func(manifestFileName string, p DBPart, wg *sync.WaitGroup, urls map[string]DXDownloadURL, mutex *sync.Mutex)

func recoverer(maxPanics int, downloadPart downloader, manifestFileName string, p DBPart, wg *sync.WaitGroup, urls map[string]DXDownloadURL, mutex *sync.Mutex) {
	defer func() {
		// The goroutine has panicked. Catch the error code, print it,
		// and try downloading the part again. This can be retried up to [maxPanics] times.
		if err := recover(); err != nil {
			fmt.Println(err)
			if maxPanics == 0 {
				panic("Too many attempts to restart downloading part. Please contact support@dnanexus.com for assistance.")
			} else {
				fmt.Println("Attempting to gracefully recover from error.")
				recoverer(maxPanics-1, downloadPart, manifestFileName, p, wg, urls, mutex)
			}
		}
	}()
	downloadPart(manifestFileName, p, wg, urls, mutex)
}

// DownloadManifestDB ...
func DownloadManifestDB(fname, token string, opts Opts) {
	// TODO: Update to not require manifest structure read into memory
	m := ReadManifest(fname)
	fmt.Printf("Preparing files for download\n")
	urls := PrepareFilesForDownload(m, token)
	statsFname := fname + ".stats.db"

	fmt.Printf("Downloading files using %d threads\n", opts.NumThreads)

	db, err := sql.Open("sqlite3", statsFname)
	check(err)
	cnt := queryDBIntegerResult("SELECT COUNT(*) FROM manifest_stats WHERE bytes_fetched != size", statsFname)
	check(err)
	rows, err := db.Query("SELECT * FROM manifest_stats WHERE bytes_fetched != size")

	jobs := make(chan JobInfo, cnt)

	var wg sync.WaitGroup

	for i := 1; rows.Next(); i++ {
		var p DBPart
		err = rows.Scan(&p.FileID, &p.Project, &p.FileName, &p.Folder, &p.PartID, &p.MD5, &p.Size, &p.BlockSize, &p.BytesFetched)
		check(err)
		var j JobInfo
		j.manifestFileName = fname
		j.part = p
		j.wg = &wg
		j.urls = urls
		j.tmpid = i
		jobs <- j
	}
	close(jobs)
	rows.Close()
	db.Close()

	var mutex = &sync.Mutex{}
	for w := 1; w <= opts.NumThreads; w++ {
		wg.Add(1)
		go worker(w, jobs, token, mutex, &wg)
	}
	wg.Wait()
	fmt.Println("")
}

// CheckFileIntegrity ...
func CheckFileIntegrity(fname string, opts Opts) {
	statsFname := fname + ".stats.db"

	db, err := sql.Open("sqlite3", statsFname)
	check(err)
	cnt := queryDBIntegerResult("SELECT COUNT(*) FROM manifest_stats WHERE bytes_fetched == size", statsFname)
	check(err)
	rows, err := db.Query("SELECT * FROM manifest_stats WHERE bytes_fetched == size")

	jobs := make(chan JobInfo, cnt)

	var wg sync.WaitGroup

	for i := 1; rows.Next(); i++ {
		var p DBPart
		err = rows.Scan(&p.FileID, &p.Project, &p.FileName, &p.Folder, &p.PartID, &p.MD5, &p.Size, &p.BlockSize, &p.BytesFetched)
		check(err)
		var j JobInfo
		j.manifestFileName = fname
		j.part = p
		j.wg = &wg
		j.tmpid = i
		jobs <- j
	}
	close(jobs)
	rows.Close()
	db.Close()

	var mutex = &sync.Mutex{}
	for w := 1; w <= opts.NumThreads; w++ {
		wg.Add(1)
		go fileIntegrityWorker(w, jobs, mutex, &wg)
	}
	wg.Wait()
	fmt.Println("")
	fmt.Println("Integrity check complete.")
}

// UpdateDBPart ...
func UpdateDBPart(manifestFileName string, p DBPart) {
	// statsFname := "file:" + manifestFileName + ".stats.db?cache=shared&mode=rwc"
	statsFname := manifestFileName + ".stats.db?_busy_timeout=60000&cache=shared&mode=rwc"
	db, err := sql.Open("sqlite3", statsFname)
	check(err)
	defer db.Close()
	tx, err := db.Begin()
	check(err)
	defer tx.Commit()

	_, err = tx.Exec(fmt.Sprintf("UPDATE manifest_stats SET bytes_fetched = %d WHERE file_id = '%s' AND part_id = '%d'", p.Size, p.FileID, p.PartID))
	check(err)

}

// ResetDBPart ...
func ResetDBPart(manifestFileName string, p DBPart) {
	// statsFname := "file:" + manifestFileName + ".stats.db?cache=shared&mode=rwc"
	statsFname := manifestFileName + ".stats.db?_busy_timeout=60000&cache=shared&mode=rwc"
	db, err := sql.Open("sqlite3", statsFname)
	check(err)
	defer db.Close()
	tx, err := db.Begin()
	check(err)
	defer tx.Commit()

	_, err = tx.Exec(fmt.Sprintf("UPDATE manifest_stats SET bytes_fetched = 0 WHERE file_id = '%s' AND part_id = '%d'", p.FileID, p.PartID))
	check(err)

}

// ResetDBFile ...
func ResetDBFile(manifestFileName string, p DBPart) {
	// statsFname := "file:" + manifestFileName + ".stats.db?cache=shared&mode=rwc"
	statsFname := manifestFileName + ".stats.db?_busy_timeout=60000&cache=shared&mode=rwc"
	db, err := sql.Open("sqlite3", statsFname)
	check(err)
	defer db.Close()
	tx, err := db.Begin()
	check(err)
	defer tx.Commit()

	_, err = tx.Exec(fmt.Sprintf("UPDATE manifest_stats SET bytes_fetched = 0 WHERE file_id = '%s'", p.FileID))
	check(err)

}

// DownloadDBPart ...
func DownloadDBPart(manifestFileName string, p DBPart, wg *sync.WaitGroup, urls map[string]DXDownloadURL, mutex *sync.Mutex) {
	fname := fmt.Sprintf(".%s/%s", p.Folder, p.FileName)
	localf, err := os.OpenFile(fname, os.O_WRONLY, 0777)
	check(err)
	headers := make(map[string]string)
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", (p.PartID-1)*p.BlockSize, p.PartID*p.BlockSize-1)
	// TODO: Avoid locking here?
	mutex.Lock()
	u := urls[p.FileID]
	mutex.Unlock()
	for k, v := range u.Headers {
		headers[k] = v
	}
	_, body := makeRequestWithHeadersFail("GET", u.URL+"/"+p.Project, headers, []byte("{}"))
	if md5str(body) != p.MD5 {
		panic(fmt.Sprintf("MD5 string of part ID %d does not match stored MD5sum", p.PartID))
	}
	_, err = localf.Seek(int64((p.PartID-1)*p.BlockSize), 0)
	check(err)
	_, err = localf.Write(body)
	check(err)
	localf.Close()
	// TODO: This lock should not be required ideally. I don't know why sqlite3 is complaining here
	mutex.Lock()
	UpdateDBPart(manifestFileName, p)
	mutex.Unlock()
}

// CheckDBPart ...
func CheckDBPart(manifestFileName string, p DBPart, wg *sync.WaitGroup, mutex *sync.Mutex) {
	fname := fmt.Sprintf(".%s/%s", p.Folder, p.FileName)
	if _, err := os.Stat(fname); os.IsNotExist(err) {
		mutex.Lock()
		ResetDBFile(manifestFileName, p)
		mutex.Unlock()
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
			// TODO: This lock should not be required ideally. I don't know why sqlite3 is complaining here
			fmt.Printf("Identified md5sum mismatch for %s part %d. Please re-issue the download command to resolve.\n", p.FileName, p.PartID)
			mutex.Lock()
			ResetDBPart(manifestFileName, p)
			mutex.Unlock()
		}
	}
}
