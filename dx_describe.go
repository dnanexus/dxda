package dxda

import (
	"context"
	"encoding/json"

	"github.com/hashicorp/go-retryablehttp" // use http libraries from hashicorp for implement retry logic
)

// Limit on the number of objects that the bulk-describe API can take
const (
	maxNumObjectsInDescribe = 1000
)

// Description of a DNAx data object
type DxDescribeDataObject struct {
	Id             string
	ProjId         string
	Name           string
	State          string
	ArchivalState  string
	Folder         string
	Size           int64
	Symlink        string   // the empty string for regular files, a full URL for symbolic links
	Parts          map[string]DXPart
}

// description of part of a file
type DXPart struct {
	MD5  string `json:"md5"`
	Size int    `json:"size"`
}



type Request struct {
	Objects []string `json:"objects"`
	ClassDescribeOptions map[string]map[string]map[string]bool `json:"classDescribeOptions"`
}

type Reply struct {
	Results []DxDescribeRawTop `json:"results"`
}

type DxDescribeRawTop struct {
	Describe DxDescribeRaw `json:"describe"`
}

type DxSymLink struct {
	Url string  `json:"object"`
}

type DxDescribeRaw struct {
	Id               string `json:"id"`
	ProjId           string `json:"project"`
	Name             string `json:"name"`
	State            string `json:"state"`
	ArchivalState    string `json:"archivalState"`
	Size             int64 `json:"size"`
	Parts            map[string]DXPart `json:"parts"`
	Symlink         *DxSymLink `json:"symlinkPath,omitempty"`
}

// Describe a large number of file-ids in one API call.
func submit(
	ctx context.Context,
	httpClient *retryablehttp.Client,
	dxEnv *DXEnvironment,
	fileIds []string) (map[string]DxDescribeDataObject, error) {

	// Limit the number of fields returned, because by default we
	// get too much information, which is a burden on the server side.
	describeOptions := map[string]map[string]map[string]bool {
		"*" : map[string]map[string]bool {
			"fields" : map[string]bool {
				"id" : true,
				"project" : true,
				"name" : true,
				"state" : true,
				"archivalState" : true,
				"size" : true,
				"symlinkPath" : true,
				"drive" : true,
				"parts" : true,
			},
		},
	}
	request := Request{
		Objects : fileIds,
		ClassDescribeOptions : describeOptions,
	}
	var payload []byte
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	//fmt.Printf("payload = %s", string(payload))

	repJs, err := DxAPI(ctx, httpClient, NumRetriesDefault, dxEnv, "system/describeDataObjects", string(payload))
	if err != nil {
		return nil, err
	}
	var reply Reply
	err = json.Unmarshal(repJs, &reply)
	if err != nil {
		return nil, err
	}

	var files = make(map[string]DxDescribeDataObject)
	for _, descRawTop := range(reply.Results) {
		descRaw := descRawTop.Describe

		symlinkUrl := ""
		if descRaw.Symlink != nil {
			symlinkUrl = descRaw.Symlink.Url
		}

		desc := DxDescribeDataObject{
			Id :  descRaw.Id,
			ProjId : descRaw.ProjId,
			Name : descRaw.Name,
			State : descRaw.State,
			ArchivalState : descRaw.ArchivalState,
			Size : descRaw.Size,
			Parts : descRaw.Parts,
			Symlink : symlinkUrl,
		}
		//fmt.Printf("%v\n", desc)
		files[desc.Id] = desc
	}
	return files, nil
}

func DxDescribeBulkObjects(
	ctx context.Context,
	httpClient *retryablehttp.Client,
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

	for _, objIdBatch := range(batches) {
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
