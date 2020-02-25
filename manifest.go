package dxda

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"sort"
	"strings"
)


// Manifest - core type of manifest file
type Manifest map[string][]*DXFile

// DXFile ...
type DXFile struct {
	// compulsory fields
	Folder        string            `json:"folder"`
	Id            string            `json:"id"`
	Name          string            `json:"name"`

	// optional parts that will get filled in by calling DNAx,
	// if they are not provided in the manifest file.
	Parts         map[string]DXPart `json:"parts,omitempty"`
	State         string            `json:"state,omitempty"`
	ArchivalState string            `json:"archivalState,omitempty"`
	Size          int64             `json:"size,omitempty"`
}

// write a log message, and add a header
func (m Manifest) log(a string, args ...interface{}) {
	LogMsg("manifest", a, args...)
}

func validateDirName(p string) error {
	dirNameLen := len(p)
	switch dirNameLen {
	case 0:
		return fmt.Errorf("the directory cannot be empty %s", p)
	default:
		if p[0] != '/' {
			return fmt.Errorf("the directory name must start with a slash %s", p)
		}
	}
	return nil
}

func validProject(pId string) bool {
	if strings.HasPrefix(pId, "project-") {
		return true
	}
	if strings.HasPrefix(pId, "container-") {
		return true
	}
	return false
}

func (m *Manifest) validate() error {
	for projId, files := range m {
		if !validProject(projId) {
			return fmt.Errorf("project has invalid Id %s", projId)
		}

		for _, fl := range files {
			if !strings.HasPrefix(fl.Id, "file-") {
				return fmt.Errorf("file has invalid Id %s", fl.FileId)
			}
			if err := validateDirName(fl.Folder); err != nil {
				return err
			}
		}
	}
	return nil
}


// Get rid of spurious slashes. For example, replace "//" with "/".
func (m *Manifest) cleanPaths() {
	for projId, files := range m {
		for i, _ := range files {
			f := files[i]
			f.Folder = filepath.Clean(f.Folder)
		}
	}
}

// fill in missing fields for each file.
//
func (m *Manifest) fillInMissingFields(ctx context.Context, dxEnv *dxda.DXEnvironment) error {
	tmpHttpClient := dxda.NewHttpClient(false)
	defer tmpHttpClient.Close()

	// Make a list of all the files that are missing details
	var fileIds []string
	for _, files := range m {
		for _, f := range m.Files {
			if f.Size == 0 {
				// the file is empty, we will be able
				// to create it without performing any DNAx reads.
				continue
			}
			// we now know that the file has at least one part
			if size(f.Parts) == 0 ||
				f.State == "" ||
				f.ArchivalState == "" {
				fileIds = append(fileIds, f.Id)
			}
		}
	}

	// describe all the files with missing elements
	dataObjs, err := DxDescribeBulkObjects(ctx, tmpHttpClient, dxEnv, fileIds)
	if err != nil {
		return err
	}

	// fill in the missing information
	for projId, files := range m {
		for i,_ := range files {
			f := files[i]
			fDesc, ok := dataObjs[f.Id]
			if !ok {
				return fmt.Errorf("File %s was not described", fDesc.Id)
			}

			if fDesc.State != "closed" {
				return fmt.Errorf("File %s is not closed, it is %s",
					fDesc.Id, fDesc.State)
			}
			if fDesc.ArchivalState != "live" {
				return fmt.Errorf("File %s is not live, it cannot be read (state=%s)",
					fDesc.Id, fDesc.State)
			}

			// This file was missing details
			f.State = fDesc.State
			f.ArchivalState = fDesc.ArchivalState
			f.Size = fDesc.Size
			f.Parts = fDesc.Parts
		}
	}

	return nil
}

// read the manifest from a file into a memory structure
func ReadManifest(fname string, dxEnv *DXEnvironment) (*Manifest, error) {
	bzdata, err := ioutil.ReadFile(fname)
	check(err)
	br := bzip2.NewReader(bytes.NewReader(bzdata))
	data, err := ioutil.ReadAll(br)
	check(err)
	var mRaw Manifest
	if err := json.Unmarshal(data, &mRaw); err != nil {
		return nil, err
	}
	m := &mRaw
	if err := m.validate(); err != nil {
		return nil, err
	}
	m.cleanPaths()

	ctx := context.TODO()
	m.fillInMissingFields(ctx, *dxEnv)

	return m, nil
}
