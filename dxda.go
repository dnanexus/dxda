package dxda

// Some inspiration + code snippets taken from https://github.com/dnanexus/precision-fda/blob/master/go/pfda.go

// TODO: Some more code cleanup + consistency with best Go practices, add more unit tests, setup deeper integration tests, add build notes
import (
	"bytes"
	"compress/bzip2"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
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

	client := &retryablehttp.Client{
		HTTPClient:   cleanhttp.DefaultClient(),
		Logger:       log.New(ioutil.Discard, "", 0), // Throw away retryablehttp internal logging
		RetryWaitMin: minRetryTime * time.Second,
		RetryWaitMax: maxRetryTime * time.Second,
		RetryMax:     maxRetryCount,
		CheckRetry:   retryablehttp.DefaultRetryPolicy,
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
	url := fmt.Sprintf("https://api.dnanexus.com/%s", api)
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
	statsFname := fname + ".stats.db"
	os.Remove(statsFname)
	db, err := sql.Open("sqlite3", statsFname)
	check(err)
	defer db.Close()
	check(err)
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

	// TODO: May want to convert this to a bulk load?
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
}

// PrepareFilesForDownload ...
// TODO: Optimize this for only files that need to be downloaded
func PrepareFilesForDownload(m Manifest, token string) map[string]DXDownloadURL {
	urls := make(map[string]DXDownloadURL)
	for _, files := range m {
		for _, f := range files {

			// Create directory structure and initialize file if it doesn't exist
			fname := fmt.Sprintf(".%s/%s", f.Folder, f.Name)
			if _, err := os.Stat(fname); os.IsNotExist(err) {
				err := os.MkdirAll(f.Folder, 0777)
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

// Probably a better way to do this :)
func queryDBIntegerResult(query, dbFname string) int {
	db, err := sql.Open("sqlite3", dbFname)
	check(err)

	rows, err := db.Query(query)
	check(err)
	var cnt int
	rows.Next()
	rows.Scan(&cnt)
	rows.Close()
	return cnt
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

func worker(id int, jobs <-chan JobInfo, token string, mutex *sync.Mutex) {
	var wg *sync.WaitGroup
	for j := range jobs {
		wg = j.wg
		if _, ok := j.urls[j.part.FileID]; !ok {
			payload := fmt.Sprintf("{\"project\": \"%s\"}", j.part.Project)
			_, body := DXAPI(token, fmt.Sprintf("%s/download", j.part.FileID), payload)
			var u DXDownloadURL
			json.Unmarshal(body, &u)
			mutex.Lock()
			j.urls[j.part.FileID] = u
			mutex.Unlock()
		}
		DownloadDBPart(j.manifestFileName, j.part, j.wg, j.urls)
		fmt.Printf("%s\r", DownloadProgress(j.manifestFileName))
	}
	wg.Done()
}

// DownloadManifestDB ...
func DownloadManifestDB(fname, token string, opts Opts) {
	m := ReadManifest(fname)
	urls := PrepareFilesForDownload(m, token)
	statsFname := fname + ".stats.db"

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
		go worker(w, jobs, token, mutex)
	}
	wg.Wait()
	fmt.Println("")
}

// UpdateDBPart ...
func UpdateDBPart(manifestFileName string, p DBPart) {
	statsFname := manifestFileName + ".stats.db?cache=shared&mode=rwc"
	db, err := sql.Open("sqlite3", statsFname)
	check(err)
	defer db.Close()
	check(err)
	_, err = db.Exec(fmt.Sprintf("UPDATE manifest_stats SET bytes_fetched = %d WHERE file_id = '%s' AND part_id = '%d'", p.Size, p.FileID, p.PartID))
	check(err)
}

// DownloadDBPart ...
func DownloadDBPart(manifestFileName string, p DBPart, wg *sync.WaitGroup, urls map[string]DXDownloadURL) {
	fname := fmt.Sprintf(".%s/%s", p.Folder, p.FileName)
	localf, err := os.OpenFile(fname, os.O_WRONLY, 0777)
	check(err)
	headers := map[string]string{
		// TODO modify ranges for for last part for ostensible correctness (works regardless in practice)
		"Range": fmt.Sprintf("bytes=%d-%d", (p.PartID-1)*p.BlockSize, p.PartID*p.BlockSize-1),
	}
	u := urls[p.FileID]
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
	UpdateDBPart(manifestFileName, p)
}
