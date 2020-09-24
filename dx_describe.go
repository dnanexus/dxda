package dxda

import (
	"context"
	"encoding/json"
	"net/http"
)

// Limit on the number of objects that the bulk-describe API can take
const (
	maxNumObjectsInDescribe = 1000
	numRetriesDefault       = 10
)

// Description of a DNAx data object
type DxDescribeDataObject struct {
	Id            string
	ProjId        string
	Name          string
	State         string
	ArchivalState string
	Folder        string
	Size          int64
	Parts         map[string]DXPart // a list of parts for a DNAx file
	Symlink       *DXSymlink
}

// description of part of a file
type DXPart struct {
	// we add the part-id in a post-processing step
	Id int

	// these fields are in the input JSON
	MD5  string `json:"md5"`
	Size int    `json:"size"`
}

// a full URL for symbolic links, with a corresponding MD5 checksum for
// the entire file.
// Drive and MD5 of symlnk
type DXSymlink struct {
	Drive string
	MD5   string
}

type Request struct {
	Objects              []string                              `json:"objects"`
	ClassDescribeOptions map[string]map[string]map[string]bool `json:"classDescribeOptions"`
}

type Reply struct {
	Results []DxDescribeRawTop `json:"results"`
}

type DxDescribeRawTop struct {
	Describe DxDescribeRaw `json:"describe"`
}

type DxSymlinkRaw struct {
	Url string `json:"object"`
}

type DxDescribeRaw struct {
	Id            string            `json:"id"`
	ProjId        string            `json:"project"`
	Name          string            `json:"name"`
	State         string            `json:"state"`
	ArchivalState string            `json:"archivalState"`
	Size          int64             `json:"size"`
	Parts         map[string]DXPart `json:"parts"`
	Symlink       *DxSymlinkRaw     `json:"symlinkPath,omitempty"`
	MD5           *string           `json:"md5,omitempty"`
	Drive         *string           `json:"drive,omitempty"`
}

// Describe a large number of file-ids in one API call.
func submit(
	ctx context.Context,
	httpClient *http.Client,
	dxEnv *DXEnvironment,
	fileIds []string) (map[string]DxDescribeDataObject, error) {

	// Limit the number of fields returned, because by default we
	// get too much information, which is a burden on the server side.
	describeOptions := map[string]map[string]map[string]bool{
		"*": map[string]map[string]bool{
			"fields": map[string]bool{
				"id":            true,
				"project":       true,
				"name":          true,
				"state":         true,
				"archivalState": true,
				"size":          true,
				"parts":         true,
				"symlinkPath":   true,
				"drive":         true,
				"md5":           true,
			},
		},
	}
	request := Request{
		Objects:              fileIds,
		ClassDescribeOptions: describeOptions,
	}
	var payload []byte
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	//fmt.Printf("payload = %s", string(payload))

	repJs, err := DxAPI(ctx, httpClient, numRetriesDefault, dxEnv, "system/describeDataObjects", string(payload))

	if err != nil {
		return nil, err
	}
	var reply Reply
	err = json.Unmarshal(repJs, &reply)
	if err != nil {
		return nil, err
	}

	var files = make(map[string]DxDescribeDataObject)
	for _, descRawTop := range reply.Results {
		descRaw := descRawTop.Describe

		// If this is a symlink, create structure with
		// all the relevant information.
		var symlink *DXSymlink = nil
		if descRaw.Drive != nil {
			symlink = &DXSymlink{
				MD5:   *descRaw.MD5,
				Drive: *descRaw.Drive,
			}
		}

		desc := DxDescribeDataObject{
			Id:            descRaw.Id,
			ProjId:        descRaw.ProjId,
			Name:          descRaw.Name,
			State:         descRaw.State,
			ArchivalState: descRaw.ArchivalState,
			Size:          descRaw.Size,
			Parts:         descRaw.Parts,
			Symlink:       symlink,
		}
		//fmt.Printf("%v\n", desc)
		files[desc.Id] = desc
	}
	return files, nil
}

func DxDescribeBulkObjects(
	ctx context.Context,
	httpClient *http.Client,
	dxEnv *DXEnvironment,
	objIds []string) (map[string]DxDescribeDataObject, error) {
	var gMap = make(map[string]DxDescribeDataObject)
	if len(objIds) == 0 {
		return gMap, nil
	}

	// split into limited batchs
	batchSize := maxNumObjectsInDescribe
	var batches [][]string

	for batchSize < len(objIds) {
		head := objIds[0:batchSize:batchSize]
		objIds = objIds[batchSize:]
		batches = append(batches, head)
	}
	// Don't forget the tail of the requests, that is smaller than the batch size
	batches = append(batches, objIds)

	for _, objIdBatch := range batches {
		m, err := submit(ctx, httpClient, dxEnv, objIdBatch)
		if err != nil {
			return nil, err
		}

		// add the results to the total result map
		for key, value := range m {
			gMap[key] = value
		}
	}
	return gMap, nil
}
