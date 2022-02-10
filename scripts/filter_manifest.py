import argparse
from pprint import pprint
import json
import bz2
import re
import os
import sys

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
def main():
    parser = argparse.ArgumentParser(description='Filter a manifest file by a regular expression on file path (folder and name)')
    parser.add_argument('manifest_file', help='BZIP2-compressed JSON manifest')
    parser.add_argument('regex', help="Regular expression")
    parser.add_argument('-o', '--output_file', required=False, default='filtered_manifest.json.bz2', help="Output file name")
    args = parser.parse_args()

    with open(args.manifest_file, "rb") as mf:
        manifest = json.loads(bz2.decompress(mf.read()))
    new_manifest = {}
    for project, file_list in manifest.items():
        new_manifest[project] = [f for f in file_list if re.match(args.regex, os.path.join(f['folder'], f['name']))]

    write_manifest_to_file(args.output_file, new_manifest)


if __name__ == "__main__":
    main()
