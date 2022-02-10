import argparse
import bz2
import json
import sys
import collections
import os
import dxpy
from dxpy.utils.resolver import resolve_existing_path

# Provide compatibility between python 2 and 3
USING_PYTHON2 = True if sys.version_info < (3, 0) else False

def write_manifest_to_file(outfile, manifest):
    if USING_PYTHON2:
        with open(outfile, "w") as f:
            f.write(bz2.compress(json.dumps(manifest, indent=2, sort_keys=True)))
    else:
        # python 3 requires opening the file in binary mode
        with open(outfile, "wb") as f:
            value = json.dumps(manifest, indent=2, sort_keys=True)
            f.write(bz2.compress(value.encode()))

def fileID2manifest(fdetails, project):
    """
    Convert a single file ID to an entry in the manifest file
    Inputs: DNAnexus file and project ID
    Output: dictionary corresponding to manifest entry
    """
    if not fdetails:
        raise "Describe output for a file is None"
    pruned = {}
    pruned['id'] = fdetails['id']
    pruned['name'] = fdetails['name']
    pruned['folder'] = fdetails['folder']
     # Symlinks do not contain parts
    if fdetails['parts']:
        pruned['parts'] = {pid: {k:v for k,v in pdetails.items() if k == "md5" or k == "size"} for pid, pdetails in fdetails['parts'].items()}
    return pruned

def main():
    parser = argparse.ArgumentParser(description='Create a manifest file for a particular folder in a project')
    parser.add_argument('folder', help='a folder in the current DNAnexus project')
    parser.add_argument('-o', '--output_file', help='Name of the output file', default='manifest.json.bz2')
    parser.add_argument('-r', '--recursive', help='Recursively traverse folders and append to manifest', action='store_true', default=False)

    args = parser.parse_args()

    project, folder, _ = resolve_existing_path(args.folder)

    ids = dxpy.find_data_objects(classname='file', first_page_size=1000, state='closed', describe={'fields': {'id': True, 'name': True, 'folder': True, 'parts': True, 'state': True, 'archivalState': True }}, project=project, folder=folder, recurse=args.recursive)
    manifest = { project: [] }

    for i,f in enumerate(ids):
        manifest[project].append(fileID2manifest(f['describe'], project))
        if i%1000 == 0 and i != 0:
            print("Processed {} files".format(i))

    # Dedup
    # Duplicate filenames are converted to filename_fileid
    dups = [item for item, count in collections.Counter([x['name'] for x in manifest[project]]).items() if count > 1]
    for x in manifest[project]:
        if x['name'] in dups:
            fname, fext = os.path.splitext(x['name'])
            x['name'] = fname + "_" + x['id'] + fext

    write_manifest_to_file(args.output_file, manifest)
    print("Manifest file written to {}".format(args.output_file))
    print("Total {} objects".format(len(manifest[project])))

if __name__ == "__main__":
    main()
