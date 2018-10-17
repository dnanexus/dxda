import argparse
import dxpy
from pprint import pprint
import json
import bz2
from dxpy.utils.resolver import resolve_existing_path


def fileID2manifest(fdetails, project):
    """
    Convert a single file ID to an entry in the manifest file
    Inputs: DNAnexus file and project ID
    Output: dictionary corresponding to manifest entry
    """
    if not fdetails:
        raise "Describe output for a file is None"

    # TODO: oddly this pruning seemed necessary when doing the system describe not perhaps not with list_folder
    pruned = {}
    pruned['id'] = fdetails['id']
    pruned['name'] = fdetails['name']
    pruned['folder'] = fdetails['folder']
    pruned['parts'] = {pid: {k:v for k,v in pdetails.items() if k == "md5" or k == "size"} for pid, pdetails in fdetails['parts'].items()}
    return pruned

def generate_manifest_file(folder, project, outfile, recursive):
  manifest = { project: [] }
  def add_folder_to_manifest(subfolder):
      print("Adding files for folder {}".format(subfolder))
      inputs = {}
      inputs['folder'] = subfolder
      inputs['describe'] = {
        'fields': {'id': True, 'name': True, 'folder': True, 'parts': True }
      }
      output = dxpy.api.project_list_folder(project, input_params=inputs)
      manifest[project] += [fileID2manifest(obj['describe'], project) for obj in output['objects']]
      if recursive:
        for subf in output['folders']:
            add_folder_to_manifest(subf)

  add_folder_to_manifest(folder)
  with open(outfile, "w") as f:
    f.write(bz2.compress(json.dumps(manifest, indent=2, sort_keys=True)))

def main():
    parser = argparse.ArgumentParser(description='Create a manifest file from a DNAnexus directory')
    parser.add_argument('directory')
    parser.add_argument('-r', '--recursive', help='Recursively traverse folders and append to manifest', action='store_true')

    parser.add_argument('--outfile', help='Name of the output file', default='manifest.json.bz2')

    args = parser.parse_args()

    project, folder, _ = resolve_existing_path(args.directory)

    generate_manifest_file(folder, project, args.outfile, args.recursive)
    print("Manifest file written to {}".format(args.outfile))

if __name__ == "__main__":
    main()
