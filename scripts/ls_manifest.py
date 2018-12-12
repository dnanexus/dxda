import argparse
from pprint import pprint
import json
import bz2
import re
import math

def r(v):
    return math.ceil(v*100)/100


def main():
    parser = argparse.ArgumentParser(description='Filter a manifest file by a regular expression on file name')
    parser.add_argument('manifest_file', help='BZIP2-compressed JSON manifest')
    parser.add_argument('regex', nargs='?', default=".*", help="Regular expression")
    parser.add_argument("--long", "-l", action='store_true', help="Long description for each file")
    args = parser.parse_args()
    
    with open(args.manifest_file) as mf:
        manifest = json.loads(bz2.decompress(mf.read()))
    new_manifest = {}

    def fsize(f):
        return sum([meta['size'] for id, meta in f['parts'].items()])

    for project, file_list in manifest.items():
        matches = [f for f in file_list if re.match(args.regex, f['name'])]
        total_size = sum([fsize(f) for f in matches])
        for f in sorted(matches, key=lambda x: x['name']):
            fol = f['folder']
            if f['folder'][-1] != "/":
                fol += "/"
            lsline = "{}{}".format(fol, f['name'])
            if args.long:
                lsline = "{:10.2f} MB\t".format(r(float(fsize(f))/(1024*1024))) + lsline
            print(lsline)
        print("")
        print("{} files total {} MB".format(len(matches), total_size/(1024*1024)))




if __name__ == "__main__":
    main()
