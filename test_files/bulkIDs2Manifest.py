import argparse
import dxpy
from pprint import pprint
import json
import bz2


def fileID2manifest(fdetails, project):
    """
    Convert a single file ID to an entry in the manifest file
    Inputs: DNAnexus file and project ID
    Output: dictionary corresponding to manifest entry
    """
    if not fdetails:
        raise "Describe output for a file is None"
    fdetails['parts'] = {pid: {k:v for k,v in pdetails.items() if k == "md5" or k == "size"} for pid, pdetails in fdetails['parts'].items()}
    return fdetails

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def generate_manifest_file(ids, project, outfile):
  manifest = { project: [] }
  for i,chunk_ids in enumerate(chunks(ids, 1000)):
      inputs = {}
      inputs['objects'] = chunk_ids
      inputs['classDescribeOptions'] = {
        'file': {'id': True, 'name': True, 'folder': True, 'parts': True }
      }
      output = dxpy.api.system_describe_data_objects(input_params=inputs)
      manifest[project] += [fileID2manifest(obj['describe'], project) for obj in output['results']]
      print( (i+1)*1000 )
  with open(outfile, "w") as f:
    f.write(bz2.compress(json.dumps(manifest, indent=2, sort_keys=True)))

def main():
    parser = argparse.ArgumentParser(description='Create a manifest file from a list of DNAnexus file IDs')
    parser.add_argument('idlist', nargs='+', help='a DNAnexus file containing a list of IDs')
    parser.add_argument('--project', help='Project ID: required to speed up API calls', required=True)
    parser.add_argument('--outfile', help='Name of the output file', default='manifest.json.bz2')

    args = parser.parse_args()

    generate_manifest_file([line.rstrip().split(":")[1] for line in open(args.idlist[0])], args.project, args.outfile)
    print("Manifest file written to {}".format(args.outfile))

if __name__ == "__main__":
    main()
