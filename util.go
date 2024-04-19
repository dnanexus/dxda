package dxda

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/pbnjay/memory"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
)
const (
	// Limit on an http request to S3/Azure object storage
	requestOverallTimout = 6 * time.Minute

	// An API request to the dnanexus servers should never take more
	// than this amount of time
	dxApiOverallTimout = 10 * time.Minute

	// Extracted automatically with a shell script, so keep the format:
	// version = XXXX
	Version = "v0.6.2"
)

// Configuration options for the download agent
type Opts struct {
	NumThreads int  // number of workers to process downloads
	Verbose    bool // verbose logging
	GcInfo     bool // Garbage collection statistics
}

// A subset of the configuration parameters that the dx-toolkit uses.
type DXEnvironment struct {
	ApiServerHost     string `json:"apiServerHost"`
	ApiServerPort     int    `json:"apiServerPort"`
	ApiServerProtocol string `json:"apiServerProtocol"`
	Token             string `json:"token"`
	DxJobId           string `json:"dxJobId"`
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
	crntDxEnv := DXEnvironment{"api.dnanexus.com", 443, "https", "", ""}

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

func check(e error) {
	if e != nil {
		panic(e)
	}
}

// Min ...
// https://mrekucci.blogspot.com/2015/07/dont-abuse-mathmax-mathmin.html
func MinInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func MinInt64(x, y int64) int64 {
	if x > y {
		return y
	}
	return x
}

func safeString2Int(s string) int {
	i, err := strconv.Atoi(s)
	check(err)
	return i
}

// print to the log and to stdout
func PrintLogAndOut(a string, args ...interface{}) {
	msg := fmt.Sprintf(a, args...)

	fmt.Print(msg)
	log.Print(msg)
}

func memorySizeBytes() int64 {
	return int64(memory.TotalMemory())
}
