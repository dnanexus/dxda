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
	token := dxda.GetToken(dxEnv)
	if token != "blah" {
		t.Errorf(fmt.Sprintf("Expected token 'blah' but got %s", token))
	}
	if method != "environment" {
		t.Errorf(fmt.Sprintf("Expected method of token retreival to be 'environment' but got %s", method))
	}

	os.Unsetenv("DX_API_TOKEN")

	// Explicitly not testing home directory config file as it may clobber existing login info for executor
}
