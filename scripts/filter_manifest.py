import argparse
from pprint import pprint
import json
import bz2
import re
import os

import compat

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

    compat.write_manifest_to_file(args.output_file, new_manifest)


if __name__ == "__main__":
    main()
