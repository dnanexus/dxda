package dxda

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"strings"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/hashicorp/go-cleanhttp"     // required by go-retryablehttp
	"github.com/hashicorp/go-retryablehttp" // use http libraries from hashicorp for implement retry logic
)

const minRetryTime = 1   // seconds
const maxRetryTime = 120 // seconds
const maxRetryCount = 10
const userAgent = "dxfs2: DNAnexus FUSE filesystem"
const reqTimeout = 15  // seconds
const maxNumAttempts = 3
const attemptTimeout = 5 // seconds

// A web status looks like: "200 OK"
// we want the 200 as an integer
func httpStatus2number(status string) int {
	elements := strings.Split(status, " ")
	if len(elements) == 0 {
		panic(fmt.Errorf("invalid status %s", status))
	}
	num, err := strconv.Atoi(elements[0])
	if err != nil {
		panic(err)
	}
	return num
}


// Good status is in the range 2xx
func isGood(status string) bool {
	statusNum := httpStatus2number(status)
	switch statusNum {
	case 200, 201, 202, 203, 204, 205, 206:
		return true
	default:
		return false
	}
}


// TODO: Investigate more sophsticated handling of these error codes ala
// https://github.com/dnanexus/dx-toolkit/blob/3f34b723170e698a594ccbea16a82419eb06c28b/src/python/dxpy/__init__.py#L655
func isRetryable(status string) bool {
	statusNum := httpStatus2number(status)
	switch statusNum {
	case 206:
		// A partial read. This is considered a failure, causing the entire
		// read to be retried.
		return true

	case 408:
		// Request timeout
		return true

	case 423:
		// Resource is locked, we can retry.
		return true

	case 429:
		// rate throttling
		return true

	case 504:
		// Gateway timeout
		return true

	default:
		return false
	}
}


// These clients are intended for reuse in the same host. Throwing them
// away will gradually leak file descriptors.
func NewHttpClient(pooled bool) *retryablehttp.Client {
	localCertFile := os.Getenv("DX_TLS_CERTIFICATE_FILE")
	if localCertFile == "" {
		client := cleanhttp.DefaultClient()
		if pooled {
			client = cleanhttp.DefaultPooledClient()
		}
		return &retryablehttp.Client{
			HTTPClient:   client,
			Logger:       log.New(ioutil.Discard, "", 0), // Throw away retryablehttp internal logging
			RetryWaitMin: minRetryTime * time.Second,
			RetryWaitMax: maxRetryTime * time.Second,
			RetryMax:     maxRetryCount,
			CheckRetry:   retryablehttp.DefaultRetryPolicy,
			Backoff:      retryablehttp.DefaultBackoff,
		}
	}

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
		log.Println("No certs appended, using system certs only")
	}

	// Trust the augmented cert pool in our client
	config := &tls.Config{
		InsecureSkipVerify: insecure,
		RootCAs:            rootCAs,
	}

	tr := cleanhttp.DefaultTransport()
	if pooled {
		tr = cleanhttp.DefaultPooledTransport()
	}
	tr.TLSClientConfig = config

	return &retryablehttp.Client{
		HTTPClient:   &http.Client{Transport: tr},
		Logger:       log.New(ioutil.Discard, "", 0), // Throw away retryablehttp internal logging
		RetryWaitMin: minRetryTime * time.Second,
		RetryWaitMax: maxRetryTime * time.Second,
		RetryMax:     maxRetryCount,
		CheckRetry:   retryablehttp.DefaultRetryPolicy,
		Backoff:      retryablehttp.DefaultBackoff,
	}
}


func dxHttpRequestCore(
	client *retryablehttp.Client,
	requestType string,
	url string,
	headers map[string]string,
	data []byte) (body []byte, err error, status string) {

	// Safety procedure to force timeout to prevent hanging
	ctx, cancel := context.WithCancel(context.TODO())
	timer := time.AfterFunc(reqTimeout * time.Second, func() {
		cancel()
	})
	req, err := retryablehttp.NewRequest(requestType, url, bytes.NewReader(data))
	if err != nil {
		return nil, err, ""
	}
	req = req.WithContext(ctx)
	for header, value := range headers {
		req.Header.Set(header, value)
	}
	resp, err := client.Do(req)
	timer.Stop()
	if err != nil {
		return nil, err, ""
	}
	status = resp.Status

	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	// If the is not in the 200-299 range, an error occured.
	if !(isGood(status)) {
		return nil, nil, status
	}

	return body, nil, status
}


// Add retries around the core http-request method
//
func DxHttpRequest(
	client *retryablehttp.Client,
	requestType string,
	url string,
	headers map[string]string,
	data []byte) (body []byte, err error) {
	tCnt := 0
	for tCnt < maxNumAttempts {
		body, err, status := dxHttpRequestCore(client, requestType, url, headers, data)
		if err != nil {
			return nil, err
		}
		if isGood(status) {
			return body, nil
		}
		err = fmt.Errorf("%s request to '%s' failed with status %s",
			requestType, url, status)
		log.Printf(err.Error() + "\n")

		// check if this is a retryable error.
		if !(isRetryable(status)) {
			return nil, err
		}

		// sleep before retrying
		time.Sleep(attemptTimeout * time.Second)
		tCnt++
	}

	err = fmt.Errorf("%s request to '%s' failed after % attempts",
		requestType, url, tCnt)
	return nil, err
}


// DxAPI - Function to wrap a generic API call to DNAnexus
func DxAPI(
	client *retryablehttp.Client,
	dxEnv *DXEnvironment,
	api string,
	payload string) (body []byte, err error) {
	if (dxEnv.Token == "") {
		err := errors.New("The token is not set. This may be because the environment isn't set.")
		return nil, err
	}
	headers := map[string]string{
		"User-Agent":   userAgent,
		"Authorization": fmt.Sprintf("Bearer %s", dxEnv.Token),
		"Content-Type":  "application/json",
	}
	url := fmt.Sprintf("%s://%s:%d/%s",
		dxEnv.ApiServerProtocol,
		dxEnv.ApiServerHost,
		dxEnv.ApiServerPort,
		api)
	return DxHttpRequest(client, "POST", url, headers, []byte(payload))
}
