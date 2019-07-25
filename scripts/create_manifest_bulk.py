import argparse
import dxpy
from pprint import pprint
import json
from dxpy.utils.resolver import resolve_existing_path
import bz2
import sys
import collections
import os

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
    pruned['parts'] = {pid: {k:v for k,v in pdetails.items() if k == "md5" or k == "size"} for pid, pdetails in fdetails['parts'].items()}
    return pruned

def main():
    parser = argparse.ArgumentParser(description='Create a manifest file for a particular folder in a project')
    parser.add_argument('folder', help='a folder in the current DNAnexus project')
    parser.add_argument('--outfile', help='Name of the output file', default='manifest.json.bz2')
    parser.add_argument('-r', '--recursive', help='Recursively traverse folders and append to manifest', action='store_true')

    args = parser.parse_args()

    project, folder, _ = resolve_existing_path(args.folder)

    ids = dxpy.find_data_objects(classname='file', first_page_size=1000, describe={'id': True, 'name': True, 'folder': True, 'parts': True }, project=project, folder=folder, recurse=args.recursive)
    manifest = { project: [] }

    for i,f in enumerate(ids):
        manifest[project].append(fileID2manifest(f['describe'], project))
        if i%1000 == 0 and i != 0:
            print("Processed {} files".format(i))

    # Dedup

    dups = [item for item, count in collections.Counter([x['name'] for x in manifest[project]]).items() if count > 1]
    for x in manifest[project]:
        if x['name'] in dups:
            fname, fext = os.path.splitext(x['name'])
            x['name'] = fname + "_" + x['id'] + fext

    with open(args.outfile, "wb") as f:
        value = json.dumps(manifest, indent=2, sort_keys=True)
        f.write(bz2.compress(value.encode()))

    print("Manifest file written to {}".format(args.outfile))
    print("Total {} objects".format(len(manifest[project])))

if __name__ == "__main__":
    main()
