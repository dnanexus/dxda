# Provide compatibility between python v2 and python v3

import bz2
import json
import sys

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
