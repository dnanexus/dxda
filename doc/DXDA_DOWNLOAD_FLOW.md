# DXDA Download Flow (Regular + Symlink)

## Purpose

This document explains how `dx-download-agent` (dxda) downloads files in the current implementation.

It covers:

1. Regular file download path
2. Symlink file download path
3. Where the flow branches
4. What is platform metadata vs local download metadata

## One-Page Summary

1. dxda reads and validates the manifest JSON.
2. If all files already include `parts`, dxda trusts those parts and skips describe.
3. If any file is missing `parts`, dxda bulk-describes files via `system/findDataObjects`.
4. After describe, dxda classifies each file:
   - Regular file: has regular `parts`
   - Symlink file: `parts` is absent and `drive` is present
5. dxda writes download work items into SQLite:
   - `manifest_regular_stats`: one row per regular part
   - `manifest_symlink_stats`: one row per local symlink chunk
   - `symlinks`: one row per symlink file (whole-file md5 metadata)
6. Workers fetch download URLs and perform HTTP `GET` with `Range`.
7. Progress and resume state are updated in SQLite.

## Manifest Input Format (Recommended)

The manifest is a JSON map from `project-xxxx` (or `container-xxxx`) to a list of files.

### A) Regular file with parts already present (trusted path eligible)

```json
{
    "project-AAAA": [
        {
            "id": "file-REG001",
            "name": "tumor.bam",
            "folder": "/bam",
            "parts": {
                "1": { "size": 10485760, "md5": "11111111111111111111111111111111" },
                "2": { "size": 5242880,  "md5": "22222222222222222222222222222222" }
            }
        }
    ]
}
```

### B) File without parts (validated path will call API bulk describe)

```json
{
    "project-AAAA": [
        {
            "id": "file-ANY001",
            "name": "needs-describe.dat",
            "folder": "/incoming"
        }
    ]
}
```

### C) Symlink-style entry (typically no parts in manifest)

```json
{
    "project-AAAA": [
        {
            "id": "file-SYM001",
            "name": "linked.vcf.gz",
            "folder": "/links"
        }
    ]
}
```

## High-Level Diagram (Easy Step-by-Step)

```mermaid
flowchart TD
    S1[Step 1: Read manifest file] --> S2[Step 2: Validate project id, file id, folder]
    S2 --> S3{Step 3: Do all files already have parts?}

    S3 -->|Yes| S4[Step 4A: Trusted path\nUse manifest parts as-is]
    S3 -->|No| S5[Step 4B: Validated path\nCall API bulk describe]

    S5 --> S6{Step 5: Classify described files}
    S6 -->|Regular| S7[Use parts returned by describe]
    S6 -->|Symlink| S8[No platform parts\nMark as symlink]

    S4 --> S9[Step 6: Build internal manifest objects in memory]
    S7 --> S9
    S8 --> S9

    S9 --> S10{Step 7: Insert download plan rows into SQLite tables by file type}
    S10 -->|Regular| S11[Insert into table manifest_regular_stats\none row per part]
    S10 -->|Symlink style metadata| S12{Are platform parts present}
    S12 -->|Yes| S12A[Treated as regular in dxda\ninsert into manifest_regular_stats\none row per platform part]
    S12 -->|No| S12B[Split into local chunks by maxChunkSize\n16 MiB off platform or 64 MiB in DX job runtime\nthen insert into manifest_symlink_stats]
    S12B --> S12D[Insert into table symlinks\nwhole-file md5 metadata]

    S11 --> S13[Step 8: Start worker system\nwith configured thread count]
    S12A --> S13
    S12D --> S13

    S13 --> W1[Step 9: Prepare URL-ready jobs\nURL workers read jobs and prepare url and headers]
    S13 --> W2[Step 10: Prepare download workers\nworkers wait for URL-ready jobs]
    W1 --> Q1[Enqueue URL-ready jobs\nshared queue jobsWithUrls]
    Q1 --> W2

    W2 --> S14[Step 11: Download chunks via HTTP GET\nwith Range header and write to disk]
    S14 --> S15[Step 12: Push completion to DB update queue\nthen update SQLite progress timestamps]
```

## Runtime Download Diagram (Workers)

Note: This diagram is a zoom-in of Step 9 through Step 12 from the High-Level Diagram.
It shows explicit branches for regular and symlink jobs.

```mermaid
flowchart TD
    A[Load incomplete jobs from SQLite tables] --> B{Classify job type}

    B -->|Regular DBPartRegular| R1[Prepare URL for regular job]
    B -->|Symlink DBPartSymlink| S1[Prepare URL for symlink job]

    R1 --> R2{Is this running in a DX job\nwith DX_DXDA_DOWNLOAD_URI set?}
    R2 -->|Yes| R3[Build URL from DX_DXDA_DOWNLOAD_URI\nand fileId/project, then add X-Authorization]
    R2 -->|No| R4[Call API file-id/download\nto get pre-authenticated download URL]

    S1 --> S2[Call API file-id/download\nstandard symlink branch DSYM or D2SYM\nto get pre-authenticated download URL]

    R3 --> Q[Enqueue URL-ready job\njobsWithUrls]
    R4 --> Q
    S2 --> Q

    Q --> D{Download worker consumes URL-ready job}
    D -->|Regular| DR[HTTP GET with Range header\nRange chunk size uses maxChunkSize\n16 MiB off platform or 64 MiB in DX job\nSub-ranges within one part are sequential in one worker\nWrite chunk to disk\nVerify md5 or checksum]
    D -->|Symlink| DS[HTTP GET with Range header\nRange chunk size uses maxChunkSize\n16 MiB off platform or 64 MiB in DX job\nEach DBPartSymlink job is one pre-split chunk range\nWrite chunk to disk]

    DR --> U[Push completion to DB update queue]
    DS --> U
    U --> V[Update SQLite row\nbytes_fetched and download_done_time]
```

## Sequence Diagram (End-to-End)

```mermaid
sequenceDiagram
    participant CLI as dx-download-agent
    participant API as DNAnexus API
    participant DB as Local SQLite
    participant DL as Download endpoint URL
    participant FS as Local filesystem

    CLI->>CLI: Read manifest + format validation
    alt All files have parts
        CLI->>CLI: genTrustedManifest
    else Some files missing parts
        CLI->>API: POST system/findDataObjects (bulk describe)
        API-->>CLI: describe results (parts for regular, marker for symlink)
        CLI->>CLI: makeValidatedManifest
    end

    CLI->>DB: CreateManifestDB
    alt Regular file
        CLI->>DB: Insert rows into manifest_regular_stats
    else Symlink file
        CLI->>DB: Split into local chunks and insert manifest_symlink_stats
        CLI->>DB: Insert whole-file metadata into symlinks
    end

    loop For each incomplete row
        alt Regular + DX_DXDA_DOWNLOAD_URI shortcut
            CLI->>CLI: Build URL from DX_DXDA_DOWNLOAD_URI + fileId/project
        else Default path (symlink uses standard DSYM or D2SYM branch)
            CLI->>API: POST file-id/download {project, duration} to get pre-authenticated download URL
            API-->>CLI: {url, headers}
        end

        CLI->>DL: GET with Range + headers (sub-ranges in one part are sequential in one worker)
        DL-->>CLI: bytes
        CLI->>FS: WriteAt(offset)
        opt Regular part
            CLI->>CLI: Verify md5/checksum
        end
        opt Symlink chunk from manifest_symlink_stats
            CLI->>CLI: One row maps to one maxChunkSize range job
        end
        CLI->>DB: Update bytes_fetched and download_done_time
    end

    CLI->>CLI: Print progress and completion
```

## Edge Case to Know

If describe returns `parts=nil` and `drive=nil`, current logic treats it as regular with zero parts. That can result in no download jobs for that file.
