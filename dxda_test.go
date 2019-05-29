package dxda_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/dnanexus/dxda"
)

func TestGetToken(t *testing.T) {
	os.Setenv("DX_API_TOKEN", "blah")
	dxEnv, method, err := dxda.GetDxEnvironment()
	if err != nil {
		t.Errorf("Encountered an error while getting the environment")
	}
	if dxEnv.Token != "blah" {
		t.Errorf(fmt.Sprintf("Expected token 'blah' but got %s", dxEnv.Token))
	}
	if method != "environment" {
		t.Errorf(fmt.Sprintf("Expected method of token retreival to be 'environment' but got %s", method))
	}

	os.Unsetenv("DX_API_TOKEN")

	// Explicitly not testing home directory config file as it may clobber existing login info for executor
}

func TestEnvironmentQuery(t *testing.T) {
	os.Setenv("DX_APISERVER_HOST", "a.b.c.com")
	os.Setenv("DX_APISERVER_PORT", "80")
	os.Setenv("DX_APISERVER_PROTOCOL", "xxyy")
	os.Setenv("DX_SECURITY_CONTEXT",
		`{"auth_token_type": "Bearer", "auth_token": "yQ2YJfyQZV74ygvvGF281P0G5bK4VZJ5VV1x6GQ4"}`)
	os.Setenv("DX_API_TOKEN", "")

	dxEnv, _, _ := dxda.GetDxEnvironment()
	if dxEnv.ApiServerHost != "a.b.c.com" {
		t.Errorf(fmt.Sprintf("Expected host 'a.b.c.com' but got %s", dxEnv.ApiServerHost))
	}
	if dxEnv.ApiServerPort != 80 {
		t.Errorf(fmt.Sprintf("Expected port '80' but got %d", dxEnv.ApiServerPort))
	}
	if dxEnv.ApiServerProtocol != "xxyy" {
		t.Errorf(fmt.Sprintf("Expected protocol 'xxyy' but got %s", dxEnv.ApiServerProtocol))
	}
	if dxEnv.Token != "yQ2YJfyQZV74ygvvGF281P0G5bK4VZJ5VV1x6GQ4" {
		t.Errorf(fmt.Sprintf("Expected token 'yQ2YJfyQZV74ygvvGF281P0G5bK4VZJ5VV1x6GQ4' but got %s", dxEnv.Token))
	}
}
