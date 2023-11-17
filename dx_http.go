package dxda

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	maxRetryCount      = 10
	reqTimeout         = 15  // seconds
	attemptTimeoutInit = 2   // seconds
	attemptTimeoutMax  = 600 // seconds
	maxSizeResponse    = 16 * 1024

	// handling the case of receiving less data than we
	// asked for
	badLengthTimeout          = 5 // seconds
	badLengthNumRetries       = 10
	contextCanceledTimeout    = 20 // seconds
	contextCanceledNumRetries = 2
)

// example 'dxda/v0.1.2 (linux)
var UserAgent = fmt.Sprintf("dxda/%s (%s)", Version, runtime.GOOS)

type HttpError struct {
	Message             []byte
	StatusCode          int
	StatusHumanReadable string
}

type DxErrorJsonInternal struct {
	EType   string `json:"type"`
	Message string `json:"message"`
}

type DxErrorJson struct {
	E DxErrorJsonInternal `json:"error"`
}

type DxError struct {
	EType                 string
	Message               string
	HttpCode              int
	HttpCodeHumanReadable string
}

// implement the error interface
func (hErr *HttpError) Error() string {
	return fmt.Sprintf("HttpError: message=%s status=%d %s",
		hErr.Message, hErr.StatusCode, hErr.StatusHumanReadable)
}
func (dxErr *DxError) Error() string {
	return fmt.Sprintf("DxError: type=%s message=%s status=%d %s",
		dxErr.EType, dxErr.Message, dxErr.HttpCode, dxErr.HttpCodeHumanReadable)
}

// A web status looks like: "200 OK"
// we want the 200 as an integer, and "OK" as a description
func parseStatus(status string) (int, string) {
	elements := strings.Split(status, " ")
	if len(elements) == 0 {
		panic(fmt.Errorf("invalid status (%s)", status))
	}
	num, err := strconv.Atoi(elements[0])
	if err != nil {
		panic(err)
	}

	var rest string
	if len(elements) > 1 {
		rest = strings.Join(elements[1:], " ")
	}
	return num, rest
}

// Good status is in the range 2xx
func isGood(status int) bool {
	return (200 <= status && status < 300)
}

func isRetryable(ctx context.Context, requestType string, status int) bool {
	// do not retry on context.Canceled or context.DeadlineExceeded
	if ctx.Err() != nil {
		return false
	}

	switch status {
	case 408:
		// Request timeout
		return true

	case 423:
		// Resource is locked, we can retry.
		return true

	case 429:
		// rate throttling
		return true

	case 500:
		// server internal error.
		//
		// This seems like a fatal error, however,
		// we have seen the platform return these errors
		// sporadically. For example, if there is a JSON parsing
		// error on the request due to corruption.
		return true

	case 503:
		// platform is temporarily down
		return true

	case 504:
		// Gateway timeout
		return true
	}

	// Bad gateway
	// Sometimes caused by s3 closing the connection to the worker's
	// download proxy. Retryable inside a job.
	if status == 502 {
		return true
	}

	if requestType == "PUT" {
		// We are uploading data.
		switch status {
		case 400:
			// The server has closed the connection prematurely.
			// On AWS, this can happen if an upload takes more than 20 seconds.
			return true
		}
	}

	return false
}

// DefaultPooledTransport returns a new http.Transport with similar default
// values to http.DefaultTransport. Do not use this for transient transports as
// it can leak file descriptors over time. Only use this for transports that
// will be re-used for the same host(s).
func defaultPooledTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ReadBufferSize:        64 * KiB,
	}
}

// These clients are intended for reuse in the same host. Throwing them
// away will gradually leak file descriptors.
func NewHttpClient() *http.Client {
	localCertFile := os.Getenv("DX_TLS_CERTIFICATE_FILE")
	if localCertFile == "" {
		return &http.Client{
			Transport: defaultPooledTransport(),
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

	tr := defaultPooledTransport()
	tr.TLSClientConfig = config
	return &http.Client{Transport: tr}
}

func dxHttpRequestCore(
	ctx context.Context,
	client *http.Client,
	requestType string,
	url string,
	headers map[string]string,
	data []byte) (*http.Response, error) {
	var dataReader io.Reader
	if data != nil {
		dataReader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, requestType, url, dataReader)
	if err != nil {
		return nil, err
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	statusCode, statusHumanReadable := parseStatus(resp.Status)

	// If the status is not in the 200-299 range, an error occured.
	if !(isGood(statusCode)) {
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		httpError := HttpError{
			Message:             body,
			StatusCode:          statusCode,
			StatusHumanReadable: statusHumanReadable,
		}
		return nil, &httpError
	}

	// good status
	return resp, nil
}

// Add retries around the core http-request method
func DxHttpRequest(
	ctx context.Context,
	client *http.Client,
	numRetries int,
	requestType string,
	URL string,
	headers map[string]string,
	data []byte) (*http.Response, error) {

	attemptTimeout := attemptTimeoutInit
	var tCnt int
	var err error

	for tCnt = 0; tCnt <= numRetries; tCnt++ {
		if tCnt > 0 {
			// sleep before retrying. Use bounded exponential backoff.
			time.Sleep(time.Duration(attemptTimeout) * time.Second)
			attemptTimeout = MinInt(2*attemptTimeout, attemptTimeoutMax)
		}
		var response *http.Response
		response, err = dxHttpRequestCore(ctx, client, requestType, URL, headers, data)
		if err == nil {
			// http request went well, return the body
			return response, nil
		}

		// triage the error
		switch err.(type) {
		case *HttpError:
			hErr := err.(*HttpError)
			if !isRetryable(ctx, requestType, hErr.StatusCode) {
				// A non-retryable error, return the http error
				return nil, hErr
			}
			// A retryable http error.
			continue
		case *url.Error:
			// Retry ECONNREFUSED, ECONNRESET
			if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) {
				continue
			} else {
				return nil, err
			}
		default:
			// Other connection error/timeout error/library error. This is non retryable
			return nil, err
		}
	}
	log.Printf("%s request to '%s' failed after %d attempts, err=%s",
		requestType, URL, tCnt, err.Error())
	return nil, err
}

// Read data from a remote URL.
//
// Add retries around the core http-request method, especially in the case of
// short reads.
func DxHttpRequestData(
	ctx context.Context,
	httpClient *http.Client,
	requestType string,
	url string,
	headers map[string]string,
	data []byte,
	dataLen int,
	memoryBuf []byte) error {

	for ccCnt := 0; ccCnt < contextCanceledNumRetries; ccCnt++ {
		// Safety procedure to force timeout to prevent hanging
		ctx2, cancel := context.WithCancel(ctx)
		timer := time.AfterFunc(requestOverallTimout, func() {
			cancel()
		})
		defer timer.Stop()

		contextCanceled := false
		bytesFetched := 0
		for i := 0; i < badLengthNumRetries; i++ {
			resp, err := DxHttpRequest(ctx2, httpClient, numRetries, requestType, url, headers, data)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					contextCanceled = true
					break
				} else {
					return err
				}
			}

			//body, _ := ioutil.ReadAll(resp.Body)
			// we are saving an allocation by using a pre-allocated
			// buffer.
			recvLen, err := io.ReadAtLeast(resp.Body, memoryBuf, dataLen)
			resp.Body.Close()

			if err != nil && errors.Is(err, context.Canceled) {
				contextCanceled = true
				bytesFetched = recvLen
				break
			}

			// check that the length is correct
			if recvLen != dataLen {
				// Note: it would be preferable to collect partial results and concatenate them.
				log.Printf("received length is wrong, got %d, expected %d. Retrying.", recvLen, dataLen)
				time.Sleep(time.Duration(badLengthTimeout) * time.Second)
				continue
			}
			return nil
		}

		if !contextCanceled {
			return fmt.Errorf("%s request to %s failed after %d attempts",
				requestType, url, badLengthNumRetries)
		}

		log.Printf("Filepart was not successfully downloaded within %.f minutes (only %d of %d bytes fetched). Retrying (attempt %d of %d).",
			requestOverallTimout.Minutes(), bytesFetched, dataLen, ccCnt+1, contextCanceledNumRetries)
		time.Sleep(time.Duration(contextCanceledTimeout) * time.Second)
	}

	return fmt.Errorf("%s request to %s failed after %d attempts with context canceled error",
		requestType, url, contextCanceledNumRetries)
}

// DxAPI - Function to wrap a generic API call to DNAnexus
func DxAPI(
	ctx context.Context,
	client *http.Client,
	numRetries int,
	dxEnv *DXEnvironment,
	api string,
	payload string) ([]byte, error) {
	if dxEnv.Token == "" {
		err := errors.New("The token is not set. This may be because the environment isn't set.")
		return nil, err
	}
	headers := map[string]string{
		"User-Agent":    UserAgent,
		"Authorization": fmt.Sprintf("Bearer %s", dxEnv.Token),
		"Content-Type":  "application/json",
	}
	url := fmt.Sprintf("%s://%s:%d/%s",
		dxEnv.ApiServerProtocol,
		dxEnv.ApiServerHost,
		dxEnv.ApiServerPort,
		api)

	// Safety procedure to force timeout to prevent hanging
	ctx2, cancel := context.WithCancel(ctx)
	timer := time.AfterFunc(dxApiOverallTimout, func() {
		cancel()
	})
	defer timer.Stop()

	resp, err := DxHttpRequest(ctx2, client, numRetries, "POST", url, headers, []byte(payload))
	if err != nil {
		switch err.(type) {
		case *HttpError:
			// unmarshal the JSON response we got from dnanexus.
			hErr := err.(*HttpError)

			var dxErr DxError
			if len(hErr.Message) < maxSizeResponse {
				var dxErrJson DxErrorJson
				if err := json.Unmarshal(hErr.Message, &dxErrJson); err != nil {
					log.Printf("could not unmarshal JSON response (%s)", hErr.Message)
				}
				dxErr.EType = dxErrJson.E.EType
				dxErr.Message = dxErrJson.E.Message
			} else {
				log.Printf("response is larger than maximum, %d > %d",
					len(hErr.Message), maxSizeResponse)
			}

			// the status can just be copied from the http error.
			// It will be usable even if we can't parse the JSON response
			dxErr.HttpCode = hErr.StatusCode
			dxErr.HttpCodeHumanReadable = hErr.StatusHumanReadable
			return nil, &dxErr
		}
		// non dnanexus error.
		return nil, err
	}
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	// good return case
	return body, nil
}
