package dxda

func

func fileIdToManifest(status int) bool {
	return (200 <= status && status < 300)
}

func findDataObjects() {
	DxAPI
}

// resolve path

// ids = dxpy.find_data_objects(classname='file', first_page_size=1000, state='closed', describe={'fields': {'id': True, 'name': True, 'folder': True, 'parts': True, 'state': True, 'archivalState': True }}, project=project, folder=folder, recurse=args.recursive)

// manifest = { project: [] }

// for i,f in enumerate(ids):
//         manifest[project].append(fileID2manifest(f['describe'], project))
//         if i%1000 == 0 and i != 0:
//             print("Processed {} files".format(i))

// dups = [item for item, count in collections.Counter([x['name'] for x in manifest[project]]).items() if count > 1]
// for x in manifest[project]:
// 	if x['name'] in dups:
// 		fname, fext = os.path.splitext(x['name'])
// 		x['name'] = fname + "_" + x['id'] + fext

// compat.write_manifest_to_file(args.output_file, manifest)

// def fileID2manifest(fdetails, project):
//     """
//     Convert a single file ID to an entry in the manifest file
//     Inputs: DNAnexus file and project ID
//     Output: dictionary corresponding to manifest entry
//     """
//     if not fdetails:
//         raise "Describe output for a file is None"
//     pruned = {}
//     pruned['id'] = fdetails['id']
//     pruned['name'] = fdetails['name']
//     pruned['folder'] = fdetails['folder']
//      # Symlinks do not contain parts
//     if fdetails['parts']:
//         pruned['parts'] = {pid: {k:v for k,v in pdetails.items() if k == "md5" or k == "size"} for pid, pdetails in fdetails['parts'].items()}
//     return pruned
