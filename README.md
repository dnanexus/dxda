# dx-download-agent
CLI tool to manage the download of large quantities of files from DNAnexus

**WARNING: This is an alpha version of this tool. It is currently in a specification/draft stage and it is likely incomplete. Please use at your own risk.**

## Quick Start

To get started with `dx-download-agent`, download the the latest pre-compiled binary from the release page.  The download agent accepts two files:

* `manifest_file`: A BZ2-compressed JSON manifest file that describes, at minimimum, the following information for a download, for example:

```json
{ 
  "project-AAAA": [
    {
      "id": "file-XXXX",
      "name": "foo",
      "folder": "/path/to",
      "parts": {
        "1": { "size": 10, "md5": "49302323" },
        "2": { "size": 5,  "md5": "39239329" }
      }
    },
    "..."
  ],
  "project-BBBB": [ "..." ]
}
```

To start a download process, first [generate a DNAnexus API token](https://wiki.dnanexus.com/Command-Line-Client/Login-and-Lgout#Authentication-Tokens) that is valid for a time period that you plan on downloading the files.  Store it in the following environment variable:

```bash
export DX_API_TOKEN=<INSERT API TOKEN HERE>
```

If no API token is provided, the download agent will look to the `~/.dnanexus_config/environment.json` also used by the [dx-toolkit](https://github.com/dnanexus/dx-toolkit),

In the same directory, you can probe your environment for download readiness with this command:

```
dx-download-agent inspect exome_bams_manifest.json.bz2
```

This command will perfrom a series of initial checks but avoid downloads.  These checks include:

* Network connectivity and potential issues with it
* Whether you have enough space locally
* Approximate speeds of download rates
* Whether it looks like another download process is running (i.e. file sizes are changing, status files being updated).

To start the download:

```
dx-download-agent download exome_bams_manifest.json.bz2
Obtained token using ~/.dnanexus_config/environment.json
100/200 MB      11/17 Parts Downloaded
```

This command will also probe the environment and, if it doesn't appear another download process is running, it will start a download process within your terminal using the current working directory.

You can query the progress of an existing download in a separate terminal

```
dx-download-agent progress exome_bams_manifest.json.bz2 
```

and you will get a brief summary of the status the downloads:

```
100/200 MB      11/17 Parts Downloaded
```

## Execution options

* `--max_threads` (integer): maximum # of concurrent threads to use when downloading files
* ...


## Manifest stats database spec

Information about what parts have been downloaded is maintained in a sqlite3 database file that contains similar information as to what's in the JSON file format plus an additional `bytes_fetched` field.

Table name: `manifest_stats`

Fields (all fields are strings unless otherwise specified)

* `file_id`: file ID for file part
* `project`: project ID for file part
* `name`: name of file
* `folder`: folder containing file on DNAnexus
* `part_id` (integer): part ID for file
* `md5`: md5sum for part ID
* `size` (integer): size of the part
* `block_size` (integer): primary block size of file (assumed equal to `size` except for the last part)
* `bytes_fetched` (integer <= `size`): total number of bytes downloaded

It is up to the implementation to decide whether or not `bytes_fetched` is updated in a more coarse- vs. fine-grained fashion.  For example, `bytes_fetched` can be updated only when the part download is complete. In this case, its values will only be `0` or the value of `size`.

## Additional notes

* Only objects of [class File](https://wiki.dnanexus.com/API-Specification-v1.0.0/Introduction-to-Data-Object-Classes) can be downloaded. 
* On DNAnexus, files are immutable and the same directory can contain multiple files of the same name.  If this occurs, files on a local POSIX filesystem will be appended with the DNAnexus file ID to ensure they are not overwritten.  
* In the case a directory and a file have the same name and share the same parent directory, a DNAnexus file ID will also be appended.  If the file name contains at least one character that is illegal on a POSIX system, the file will be named directly by its file ID on DNAnexus.
