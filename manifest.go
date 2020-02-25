package dxda

import (
	"bytes"
	"compress/bzip2"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
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
	Size          int64             `json:"size,omitempty"`
	Symlink   string
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
	for projId, files := range *m {
		if !validProject(projId) {
			return fmt.Errorf("project has invalid Id %s", projId)
		}

		for _, f := range files {
			if !strings.HasPrefix(f.Id, "file-") {
				return fmt.Errorf("file has invalid Id %s", f.Id)
			}
			if err := validateDirName(f.Folder); err != nil {
				return err
			}
		}
	}
	return nil
}


// Get rid of spurious slashes. For example, replace "//" with "/".
func (m *Manifest) cleanPaths() {
	for _, files := range *m {
		for i, _ := range files {
			f := files[i]
			f.Folder = filepath.Clean(f.Folder)
		}
	}
}

// fill in missing fields for each file.
//
func (m *Manifest) fillInMissingFields(ctx context.Context, dxEnv *DXEnvironment) error {
	tmpHttpClient := NewHttpClient(false)

	// Make a list of all the file-ids
	var fileIds []string
	for _, files := range *m {
		for _, f := range files {
			fileIds = append(fileIds, f.Id)
		}
	}

	// describe all the files
	dataObjs, err := DxDescribeBulkObjects(ctx, tmpHttpClient, dxEnv, fileIds)
	if err != nil {
		return err
	}

	// fill in the missing information
	for _, files := range *m {
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
					fDesc.Id, fDesc.ArchivalState)
			}

			// This file was missing details
			f.Parts = fDesc.Parts
			f.Size = fDesc.Size
			f.Symlink = fDesc.Symlink
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
	if err := m.fillInMissingFields(ctx, dxEnv); err != nil {
		return nil, err
	}

	return m, nil
}
