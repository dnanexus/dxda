# dx-download-agent

CLI tool to manage the download of large quantities of files from DNAnexus

[![Build Status](https://travis-ci.org/dnanexus/dxda.svg?branch=master)](https://travis-ci.org/dnanexus/dxda)

**NOTE: This is an early version of this tool and is undergoing testing in a variety of settings.  Please contact DNAnexus if you are interested in seeing if this tool is appropriate for your application.**

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

If no API token is provided, the download agent will look to the `~/.dnanexus_config/environment.json` also used by the [dx-toolkit](https://github.com/dnanexus/dx-toolkit).


To start the download:

```
dx-download-agent download exome_bams_manifest.json.bz2
Obtained token using environment
Creating manifest database manifest.json.bz2.stats.db
Required disk space = 1.2TB, available = 3.6TB
Logging detailed output to: manifest.json.bz2.download.log
Preparing files for download
Downloading files using 8 threads
Downloaded 11904/1098469 MB     124/11465 Parts (104.0 MB written to disk in the last 60s)
```

A continuous report on download progress is written to the
screen. Prior to starting the data transfer, a check is made to see
that there is sufficient disk space for the entire list of files. If
not, an error is reported, and nothing is downloaded. Download speed
reflects not only network bandwidth, but also the IO capability of your machine.

A download log contains more detailed information about the download should an error occur.  If an error does occur and you do not understand how to deal with it, please contact `support@dnanexus.com` with the log file attached and we will assist you.

You can query the progress of an existing download in a separate terminal

```
dx-download-agent progress exome_bams_manifest.json.bz2
```

and you will get a brief summary of the status the downloads:

```
21.6 MB/sec	1184/27078 MB	18/327 Parts Downloaded and written to disk
```


```
dx-download-agent inspect exome_bams_manifest.json.bz2
```

This command will perfrom an inspection of the parts of files currently downloaded to disk and ensure that their MD5sums match the manifest.  If a file is missing or an MD5sum does not match, the download agent will report the affected files.  After the inspection is complete, you can issue a `download` command again to resolve the issues.

## Execution options

* `-max_threads` (integer): maximum # of concurrent threads to use when downloading files

For example, the commmand

```
dx-download-agent download -max_threads=20 exome_bams_manifest.json.bz2
```

will create a worker pool of 20 threads that will download parts of files in parallel.  A maximum of 20 workers will perform downloads at any time.  Rate-limiting of downloads can be controlled to an extent by varying this number.


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

## Proxy and TLS settings

To direct `dx-download-agent` to a proxy, please set the `HTTP_PROXY` environment variable to something like `export HTTP_PROXY=hostname:port`.  `HTTPS_PROXY` is also supported.

By default, `dx-download-agent` uses certificates installed on the system to create secure connections.  If your system requires an additional TLS certificate and the `dx-download-agent` doesn't appear to be using a certificate installed on your system, there are two options in order of preference.  First, set the `DX_TLS_CERTIFICATE_FILE` environment variable to the path of the PEM-encoded TLS certificate file required by your parent organization. As a last-resort, you can connect insecurely by avoiding certificate verification all together by setting `DX_TLS_SKIP_VERIFY=true`. Use this for testing purposes only.

## Splitting manifest files

In some cases it may be desirable to split the download manifest into multiple manifest files for testing purposes or to manage multiple downloads of an entire data set across different environments.  To split the file, we provide a simple Python utility that requires no additional packages in the `scripts/` directory.  For example, executing the command:

```
python scripts/split_manifest.py manifest.json.bz2 -n 100
```

will create manifest files containing each containing 100 files per project.  For example if there are 300 total files in manifest.json.bz2, the output of this command will create three files named: `manifest_001.json.bz2`, `manifest_002.json.bz2`, and `manifest_003.json.bz2`.   Each of these files can be used independently with the download agent.


## Development environment and running with Docker

`dx-download-agent` is written in Go and releases of its binary are generally self-contained (i.e. you do not need extra dependencies to run the executable for your architecture).  We also provide a Dockerized version that includes the necessary dependencies to develop for `dxda` and also run it.

To execute `dx-download-agent` via its docker image, simply replace calls to `dx-download-agent ARGS` with `docker run dnanexus/dxda ARGS`.  Note that you will need to mount your local files and set appropriate environment variables to execute.  For example:

```
docker run -v $PWD:/workdir -w /workdir -e DX_API_TOKEN=$DX_API_TOKEN dnanexus/dxda:v0.1.4 download -max_threads=20 manifest.json.bz2
```

This repository can be used directly as a Go module as well.  In the `cmd/dx-download-agent` directory, the `dx-download-agent.go` file is an example of how it can be used.

For developing and experimenting with the source, the [Dockerfile](https://github.com/dnanexus/dxda/blob/master/Dockerfile) in this repository may be a good start.

## Additional notes

* Only objects of [class File](https://wiki.dnanexus.com/API-Specification-v1.0.0/Introduction-to-Data-Object-Classes) can be downloaded.
