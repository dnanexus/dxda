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

//----------------------------------------------------------------------------------
// External facing types

// Manifest.
//  1) a map from file-id to a description of a regular file
//  2) a map from file-id to a description of a symbolic link
type Manifest struct {
	Files     []DXFile
	Symlinks  []DXSymlinkFile
}

// DXFile ...
type DXFile struct {
	Folder        string
	Id            string
	ProjId        string
	Name          string
	Size          int64
	Parts         map[string]DXPart
}

type DXSymlinkFile struct {
	Folder        string
	Id            string
	ProjId        string
	Name          string
	Size          int64
	Url           string
	MD5           string
}

//----------------------------------------------------------------------------------

// Raw manifest. A list provided by the user of projects and files within them
// that need to be downloaded.
//
// The representation is a mapping from project-id to a list of files
type ManifestRaw map[string][]ManifestRawFile

// File description in the manifest. Additional details will be gathered
// with an API call.
type ManifestRawFile struct {
	Folder        string            `json:"folder"`
	Id            string            `json:"id"`
	Name          string            `json:"name"`
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

func validate(mRaw ManifestRaw) error {
	for projId, files := range mRaw {
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


// Fill in missing fields for each file. Split into symlinks, and regular files.
//
func makeManifest(ctx context.Context, dxEnv *DXEnvironment, mRaw manifestRaw) (Manifest, error) {
	tmpHttpClient := NewHttpClient(false)

	// Make a list of all the file-ids
	var fileIds []string
	for _, files := range m {
		for _, f := range files {
			fileIds = append(fileIds, f.Id)
		}
	}

	// describe all the files
	dataObjs, err := DxDescribeBulkObjects(ctx, tmpHttpClient, dxEnv, fileIds)
	if err != nil {
		return err
	}

	manifest := Manifest{
		Files : make([]DXFile),
		Symlinks : make([]DXSymlinkFile),
	}

	// fill in the missing information
	for projId, files := range mRaw {
		for _, f := range files {
			fDesc, ok := dataObjs[f.Id]
			if !ok {
				return fmt.Errorf("File %s was not described", f.Id)
			}
			if fDesc.State != "closed" {
				return fmt.Errorf("File %s is not closed, it is %s",
					f.Id, fDesc.State)
			}
			if fDesc.ArchivalState != "live" {
				return fmt.Errorf("File %s is not live, it cannot be read (state=%s)",
					f.Id, fDesc.ArchivalState)
			}

			// Get rid of spurious slashes. For example, replace "//" with "/".
			folder := filepath.Clean(f.Folder)

			if fDesc.symlink == nil {
				// regular file
				dxFile := DXFile{
					Folder : folder,
					Id : f.Id,
					ProjId : projId,
					Name : f.Name,
					Size : fDesc.Size,
					Parts : fDesc.Parts,
				}
				manifest.Files = append(manifest.Files, dxFile)
			} else {
				// symbolic link
				dxSymlink := DXSymlinkFile{
					Folder : folder,
					Id : f.Id,
					ProjId : projId,
					Name : f.Name,
					Size : fDesc.Size,
					Url : fDesc.Symlink.Url,
					MD5 : fDesc.Symlink.MD5,
				}
				manifest.Symlinks = append(manifest.Symlinks, dxSymlink)
			}
		}
	}

	return manifest
}

// read the manifest from a file into a memory structure
func ReadManifest(fname string, dxEnv *DXEnvironment) (*Manifest, error) {
	// read from disk, and open compression
	bzdata, err := ioutil.ReadFile(fname)
	check(err)
	br := bzip2.NewReader(bytes.NewReader(bzdata))
	data, err := ioutil.ReadAll(br)
	check(err)

	var mRaw ManifestRaw
	if err := json.Unmarshal(data, &mRaw); err != nil {
		return nil, err
	}

	if err := validate(m); err != nil {
		return nil, err
	}

	ctx := context.TODO()
	return makeManifest(ctx, dxEnv, mRaw)
}
