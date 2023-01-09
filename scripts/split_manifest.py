import argparse
from pprint import pprint
import json
import bz2

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def split_manifest(manifest_file, num_files):
    with open(manifest_file, "rb") as mf:
        manifest = json.loads(bz2.decompress(mf.read()))
        for project, file_list in manifest.items():
            for i, file_subset in enumerate(chunks(file_list, num_files)):
                manifest_subset = { project: file_subset }
                outfile = "{}_{:03d}.json.bz2".format(manifest_file.rstrip(".json.bz2"), i+1)
                with open(outfile, "wb") as f:
                    f.write(bz2.compress(json.dumps(manifest_subset, indent=2, sort_keys=True).encode()))


def main():
    parser = argparse.ArgumentParser(description='Split a manifest file into multiple manifests.')
    parser.add_argument('manifest_file', help='')
    parser.add_argument('-n', '--num_files', default=100, type=int, help="Number of files per manifest")
    args = parser.parse_args()
    split_manifest(args.manifest_file, args.num_files)

if __name__ == "__main__":
    main()
