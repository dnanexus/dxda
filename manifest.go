package dxda

import (
	"bytes"
	"compress/bzip2"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
)

//----------------------------------------------------------------------------------
// External facing types

// Manifest.
//  1. a map from file-id to a description of a regular file
//  2. a map from file-id to a description of a symbolic link
type Manifest struct {
	Files []DXFile
}

// one interface representing both symbolic links and data files
type DXFile interface {
	id() string
	projId() string
	folder() string
	name() string
}

// Data file on dnanexus
type DXFileRegular struct {
	Folder       string
	Id           string
	ProjId       string
	Name         string
	Size         int64
	ChecksumType *string
	Parts        []DXPart
}

func (reg DXFileRegular) id() string     { return reg.Id }
func (reg DXFileRegular) projId() string { return reg.ProjId }
func (reg DXFileRegular) folder() string { return reg.Folder }
func (reg DXFileRegular) name() string   { return reg.Name }

type DXFileSymlink struct {
	Folder string
	Id     string
	ProjId string
	Name   string
	Size   int64
	MD5    string
}

func (slnk DXFileSymlink) id() string     { return slnk.Id }
func (slnk DXFileSymlink) projId() string { return slnk.ProjId }
func (slnk DXFileSymlink) folder() string { return slnk.Folder }
func (slnk DXFileSymlink) name() string   { return slnk.Name }

//----------------------------------------------------------------------------------

// Raw manifest. A list provided by the user of projects and files within them
// that need to be downloaded.
//
// The representation is a mapping from project-id to a list of files
type ManifestRaw map[string][]ManifestRawFile

// File description in the manifest. Additional details will be gathered
// with an API call.
type ManifestRawFile struct {
	Folder       string             `json:"folder"`
	Id           string             `json:"id"`
	Name         string             `json:"name"`
	ChecksumType *string            `json:"checksumType,omitempty"`
	Parts        *map[string]DXPart `json:"parts,omitempty"`
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

func (mRaw ManifestRaw) validate() error {
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

// Check if the manifest includes only regular files with a list of parts.
// In this case, we assume they have already been validated.
func (mRaw ManifestRaw) onlyRegularFilesWithParts() bool {
	for _, files := range mRaw {
		for _, f := range files {
			if f.Parts == nil {
				// parts are missing
				return false
			}
		}
	}

	PrintLogAndOut("All files have parts, assuming they are not archived or open\n")
	return true
}

// add ids to the parts, and sort by the part-id. They
// are sorted in string lexicographically order, which is
// not what we want.
func processFileParts(orgParts map[string]DXPart) []DXPart {
	var parts []DXPart
	for partId, p := range orgParts {
		p2 := DXPart{
			Id:       safeString2Int(partId),
			MD5:      p.MD5,
			Size:     p.Size,
			Checksum: p.Checksum,
		}
		parts = append(parts, p2)
	}

	// sort monotonically by increasing part id
	sort.Slice(parts, func(i, j int) bool { return parts[i].Id < parts[j].Id })

	return parts
}

func (mRaw ManifestRaw) genTrustedManifest() (*Manifest, error) {
	manifest := Manifest{
		Files: make([]DXFile, 0),
	}

	// fill in the missing information
	for projId, files := range mRaw {
		for _, f := range files {
			// Get rid of spurious slashes. For example, replace "//" with "/".
			folder := filepath.Clean(f.Folder)
			parts := processFileParts(*f.Parts)

			// calculate file size by summing up the parts
			size := int64(0)
			for _, p := range parts {
				size += int64(p.Size)
			}

			// regular file
			dxFile := DXFileRegular{
				Folder:       folder,
				Id:           f.Id,
				ProjId:       projId,
				Name:         f.Name,
				Size:         size,
				Parts:        parts,
				ChecksumType: f.ChecksumType,
			}
			manifest.Files = append(manifest.Files, dxFile)
		}
	}

	return &manifest, nil
}

// Fill in missing fields for each file. Split into symlinks, and regular files.
func (mRaw ManifestRaw) makeValidatedManifest(ctx context.Context, dxEnv *DXEnvironment) (*Manifest, error) {
	tmpHttpClient := &http.Client{}

	var describedObjects = make(map[string]DxDescribeDataObject)
	// batch calls per project-id
	for projectId, files := range mRaw {
		var fileIds []string
		for _, f := range files {
			fileIds = append(fileIds, f.Id)
		}
		dataObjs, err := DxDescribeBulkObjects(ctx, tmpHttpClient, dxEnv, projectId, fileIds)
		if err != nil {
			return nil, err
		}
		// Append described objects from this project
		for objId, objDescribe := range dataObjs {
			describedObjects[objId] = objDescribe
		}
	}

	manifest := Manifest{
		Files: make([]DXFile, 0),
	}

	// fill in the missing information
	for projId, files := range mRaw {
		for _, f := range files {
			fDesc, ok := describedObjects[f.Id]
			if !ok {
				return nil, fmt.Errorf("File %s was not described", f.Id)
			}
			if fDesc.State != "closed" {
				return nil, fmt.Errorf("File %s is not closed, it is %s",
					f.Id, fDesc.State)
			}
			if fDesc.ArchivalState != "live" {
				return nil, fmt.Errorf("File %s is not live, it cannot be read (state=%s)",
					f.Id, fDesc.ArchivalState)
			}

			// Get rid of spurious slashes. For example, replace "//" with "/".
			folder := filepath.Clean(f.Folder)

			if fDesc.Symlink == nil {
				// regular file
				dxFile := DXFileRegular{
					Folder: folder,
					Id:     f.Id,
					ProjId: projId,
					Name:   f.Name,
					Size:   fDesc.Size,
					Parts:  processFileParts(fDesc.Parts),
				}
				manifest.Files = append(manifest.Files, dxFile)
			} else {
				// symbolic link
				dxSymlink := DXFileSymlink{
					Folder: folder,
					Id:     f.Id,
					ProjId: projId,
					Name:   f.Name,
					Size:   fDesc.Size,
					MD5:    fDesc.Symlink.MD5,
				}
				manifest.Files = append(manifest.Files, dxSymlink)
			}
		}
	}

	return &manifest, nil
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

	if err := mRaw.validate(); err != nil {
		return nil, err
	}

	if mRaw.onlyRegularFilesWithParts() {
		return mRaw.genTrustedManifest()
	}

	ctx := context.TODO()
	return mRaw.makeValidatedManifest(ctx, dxEnv)
}
