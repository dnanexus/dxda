package dxda

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
)

func parseRawManifest(t *testing.T, data string) ManifestRaw {
	t.Helper()
	var mRaw ManifestRaw
	if err := json.Unmarshal([]byte(data), &mRaw); err != nil {
		t.Fatalf("Could not unmarshal manifest: %s", err)
	}
	return mRaw
}

func onlyRegularFile(t *testing.T, m *Manifest) DXFileRegular {
	t.Helper()
	if len(m.Files) != 1 {
		t.Fatalf("Expected 1 file in the manifest, got %d", len(m.Files))
	}
	reg, ok := m.Files[0].(DXFileRegular)
	if !ok {
		t.Fatalf("Expected a regular file, got %T", m.Files[0])
	}
	return reg
}

// A manifest that already carries parts is trusted as-is. The checksum type and
// the per-part checksums must be carried into the manifest.
func TestGenTrustedManifestWithChecksumType(t *testing.T) {
	mRaw := parseRawManifest(t, `{
		"project-0001": [{
			"id": "file-0001",
			"name": "a.txt",
			"folder": "/sub",
			"checksumType": "CRC64NVME",
			"parts": {"1": {"size": 10, "checksum": "12xUBUlUwUM="}, "2": {"size": 5, "checksum": "ImIEBA=="}}
		}]
	}`)

	manifest, err := mRaw.genTrustedManifest()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	reg := onlyRegularFile(t, manifest)
	if reg.ChecksumType == nil {
		t.Fatalf("Expected ChecksumType to be set, got nil")
	}
	if *reg.ChecksumType != ChecksumCRC64NVME {
		t.Errorf("Expected ChecksumType %s, got %s", ChecksumCRC64NVME, *reg.ChecksumType)
	}
	// the size is the sum of the parts
	if reg.Size != 15 {
		t.Errorf("Expected size 15, got %d", reg.Size)
	}
	if len(reg.Parts) != 2 {
		t.Fatalf("Expected 2 parts, got %d", len(reg.Parts))
	}
	if reg.Parts[0].Checksum == nil || *reg.Parts[0].Checksum != "12xUBUlUwUM=" {
		t.Errorf("Expected part 1 checksum to be carried over, got %v", reg.Parts[0].Checksum)
	}
}

// A manifest without a checksum type is the pre-existing, md5-only shape.
func TestGenTrustedManifestWithoutChecksumType(t *testing.T) {
	mRaw := parseRawManifest(t, `{
		"project-0001": [{
			"id": "file-0001",
			"name": "a.txt",
			"folder": "/sub",
			"parts": {"1": {"size": 10, "md5": "9e107d9d372bb6826bd81d3542a419d6"}}
		}]
	}`)

	manifest, err := mRaw.genTrustedManifest()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	reg := onlyRegularFile(t, manifest)
	if reg.ChecksumType != nil {
		t.Errorf("Expected nil ChecksumType, got %q", *reg.ChecksumType)
	}
	if reg.Parts[0].MD5 != "9e107d9d372bb6826bd81d3542a419d6" {
		t.Errorf("Expected the part md5 to be carried over, got %s", reg.Parts[0].MD5)
	}
	if reg.Parts[0].Checksum != nil {
		t.Errorf("Expected nil part checksum, got %q", *reg.Parts[0].Checksum)
	}
}

// Parts are keyed by strings in the manifest, but must end up sorted
// numerically, not lexicographically.
func TestProcessFilePartsSortsNumerically(t *testing.T) {
	parts := processFileParts(map[string]DXPart{
		"1":  {Size: 1},
		"2":  {Size: 2},
		"10": {Size: 10},
		"11": {Size: 11},
	})

	var gotIds []int
	for _, p := range parts {
		gotIds = append(gotIds, p.Id)
	}
	for i, want := range []int{1, 2, 10, 11} {
		if gotIds[i] != want {
			t.Fatalf("Expected part ids [1 2 10 11], got %v", gotIds)
		}
	}
}

// The describe path must carry the checksum type into the manifest. It used to
// be dropped here, so nothing downstream could verify a non-md5 checksum.
func TestMakeValidatedManifestPropagatesChecksumType(t *testing.T) {
	dxEnv, _ := newFakeApiServer(t, describeReply(`{
		"id": "file-0001",
		"project": "project-0001",
		"name": "a.txt",
		"state": "closed",
		"archivalState": "live",
		"size": 10,
		"checksumType": "CRC32C",
		"parts": {"1": {"size": 10, "checksum": "ImIEBA=="}}
	}`))

	mRaw := parseRawManifest(t, `{"project-0001": [{"id": "file-0001", "name": "a.txt", "folder": "/sub"}]}`)
	manifest, err := mRaw.makeValidatedManifest(context.Background(), dxEnv)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	reg := onlyRegularFile(t, manifest)
	if reg.ChecksumType == nil {
		t.Fatalf("Expected ChecksumType to be set, got nil")
	}
	if *reg.ChecksumType != ChecksumCRC32C {
		t.Errorf("Expected ChecksumType %s, got %s", ChecksumCRC32C, *reg.ChecksumType)
	}
	if reg.Parts[0].Checksum == nil || *reg.Parts[0].Checksum != "ImIEBA==" {
		t.Errorf("Expected the part checksum to be carried over, got %v", reg.Parts[0].Checksum)
	}
	// the folder comes from the manifest, not from the describe reply. It is
	// normalised for the local filesystem, so the separator is platform
	// dependent.
	if want := filepath.Clean("/sub"); reg.Folder != want {
		t.Errorf("Expected folder %s, got %s", want, reg.Folder)
	}
}

// The describe path must tolerate a reply with no checksum type at all.
func TestMakeValidatedManifestWithoutChecksumType(t *testing.T) {
	dxEnv, _ := newFakeApiServer(t, describeReply(`{
		"id": "file-0001",
		"project": "project-0001",
		"name": "a.txt",
		"state": "closed",
		"archivalState": "live",
		"size": 10,
		"parts": {"1": {"size": 10, "md5": "9e107d9d372bb6826bd81d3542a419d6"}}
	}`))

	// a single leading slash, so that normalisation is unambiguous on Windows
	// too: a doubled leading slash is a UNC root there, not a spurious slash
	const rawFolder = "/sub//nested"
	mRaw := parseRawManifest(t, `{"project-0001": [{"id": "file-0001", "name": "a.txt", "folder": "`+rawFolder+`"}]}`)
	manifest, err := mRaw.makeValidatedManifest(context.Background(), dxEnv)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	reg := onlyRegularFile(t, manifest)
	if reg.ChecksumType != nil {
		t.Errorf("Expected nil ChecksumType, got %q", *reg.ChecksumType)
	}
	// spurious slashes are cleaned up
	if reg.Folder == rawFolder {
		t.Errorf("Expected the folder to be normalised, got the raw %s", reg.Folder)
	}
	if want := filepath.Clean(rawFolder); reg.Folder != want {
		t.Errorf("Expected folder %s, got %s", want, reg.Folder)
	}
}

// A symlink has no parts and no checksum type, only a whole-file md5.
func TestMakeValidatedManifestSymlink(t *testing.T) {
	dxEnv, _ := newFakeApiServer(t, describeReply(`{
		"id": "file-0002",
		"project": "project-0001",
		"name": "linked.txt",
		"state": "closed",
		"archivalState": "live",
		"size": 20,
		"drive": "drive-0001",
		"md5": "9e107d9d372bb6826bd81d3542a419d6",
		"symlinkPath": {"object": "https://example.invalid/linked.txt"}
	}`))

	mRaw := parseRawManifest(t, `{"project-0001": [{"id": "file-0002", "name": "linked.txt", "folder": "/sub"}]}`)
	manifest, err := mRaw.makeValidatedManifest(context.Background(), dxEnv)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(manifest.Files) != 1 {
		t.Fatalf("Expected 1 file, got %d", len(manifest.Files))
	}
	slnk, ok := manifest.Files[0].(DXFileSymlink)
	if !ok {
		t.Fatalf("Expected a symlink, got %T", manifest.Files[0])
	}
	if slnk.MD5 != "9e107d9d372bb6826bd81d3542a419d6" {
		t.Errorf("Unexpected symlink md5 %s", slnk.MD5)
	}
	if slnk.Size != 20 {
		t.Errorf("Expected size 20, got %d", slnk.Size)
	}
}

// Files that cannot be downloaded must be rejected with an error rather than
// silently included.
func TestMakeValidatedManifestRejectsUnreadableFiles(t *testing.T) {
	for _, tc := range []struct {
		name     string
		state    string
		archival string
	}{
		{"open file", "open", "live"},
		{"archived file", "closed", "archived"},
	} {
		dxEnv, _ := newFakeApiServer(t, describeReply(`{
			"id": "file-0001",
			"project": "project-0001",
			"name": "a.txt",
			"state": "`+tc.state+`",
			"archivalState": "`+tc.archival+`",
			"size": 10,
			"parts": {"1": {"size": 10, "md5": "9e107d9d372bb6826bd81d3542a419d6"}}
		}`))

		mRaw := parseRawManifest(t, `{"project-0001": [{"id": "file-0001", "name": "a.txt", "folder": "/sub"}]}`)
		if _, err := mRaw.makeValidatedManifest(context.Background(), dxEnv); err == nil {
			t.Errorf("%s: expected an error, got nil", tc.name)
		}
	}
}

// A file that the platform did not return at all must be reported.
func TestMakeValidatedManifestUndescribedFile(t *testing.T) {
	dxEnv, _ := newFakeApiServer(t, `{"results":[]}`)

	mRaw := parseRawManifest(t, `{"project-0001": [{"id": "file-0001", "name": "a.txt", "folder": "/sub"}]}`)
	if _, err := mRaw.makeValidatedManifest(context.Background(), dxEnv); err == nil {
		t.Fatalf("Expected an error for an undescribed file, got nil")
	}
}

// A manifest is only trusted when every file carries parts, otherwise the
// missing details have to be fetched from the platform.
func TestOnlyRegularFilesWithParts(t *testing.T) {
	withParts := parseRawManifest(t, `{
		"project-0001": [{"id": "file-0001", "name": "a.txt", "folder": "/sub", "parts": {"1": {"size": 1}}}]
	}`)
	if !withParts.onlyRegularFilesWithParts() {
		t.Errorf("Expected a manifest with parts to be trusted")
	}

	mixed := parseRawManifest(t, `{
		"project-0001": [
			{"id": "file-0001", "name": "a.txt", "folder": "/sub", "parts": {"1": {"size": 1}}},
			{"id": "file-0002", "name": "b.txt", "folder": "/sub"}
		]
	}`)
	if mixed.onlyRegularFilesWithParts() {
		t.Errorf("Expected a manifest with a partless file to require a describe call")
	}
}

func TestManifestRawValidate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{"valid project", `{"project-0001": [{"id": "file-0001", "name": "a", "folder": "/sub"}]}`, false},
		{"valid container", `{"container-0001": [{"id": "file-0001", "name": "a", "folder": "/"}]}`, false},
		{"bad project id", `{"file-0001": [{"id": "file-0001", "name": "a", "folder": "/sub"}]}`, true},
		{"bad file id", `{"project-0001": [{"id": "record-0001", "name": "a", "folder": "/sub"}]}`, true},
		{"empty folder", `{"project-0001": [{"id": "file-0001", "name": "a", "folder": ""}]}`, true},
		{"relative folder", `{"project-0001": [{"id": "file-0001", "name": "a", "folder": "sub"}]}`, true},
	} {
		err := parseRawManifest(t, tc.raw).validate()
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: expected error=%v, got %v", tc.name, tc.wantErr, err)
		}
	}
}
