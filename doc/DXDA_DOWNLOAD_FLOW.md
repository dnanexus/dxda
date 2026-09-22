# DXDA Download Flow (Regular and Symlink)

## Purpose

This document explains the current DXDA download flow end to end, including:

1. How manifest input is interpreted
2. How regular and symlink files are classified
3. How jobs are materialized into SQLite
4. How URL preparation and ranged download happen at runtime
5. How large regular parts are split into persisted chunk jobs with finalize integrity

## One-Page Summary

1. DXDA reads and validates the manifest.
2. If every file already has parts, DXDA uses trusted mode and skips bulk describe.
3. If any file is missing parts, DXDA bulk describes via `system/findDataObjects`.
4. Classification after describe is shape based:
   - Symlink if `parts == nil` and `drive != nil`
   - Regular otherwise
5. SQLite planning tables are populated:
    - `manifest_regular_stats`: one row per platform part, with `split_mode` and `integrity_done`
    - `manifest_regular_chunk_stats`: one row per persisted chunk for large regular parts
    - `manifest_symlink_stats`: one row per local chunk for symlink objects without platform parts
    - `symlinks`: one row per symlink object for whole file metadata
6. URL workers prepare pre-authenticated download URLs.
7. Download workers convert each planned part or chunk row (`offset`, `size`) into a byte range (`Range: bytes=start-end`), then issue HTTP `GET` and write to local offsets.
8. For chunked regular parts, DXDA finalizes at part level only after all chunk rows complete and integrity passes.
9. Progress and resume state are updated in SQLite.

## Manifest Input Format

The manifest is a map from `project-xxxx` or `container-xxxx` to a list of files.

Expected file format in practice:

1. Recommended filename suffix: `*.json.bz2`.
2. DXDA does not enforce the suffix string directly.
3. DXDA always reads the file as BZip2-compressed bytes and then parses JSON.
4. So the actual content must be valid `BZip2 + JSON`, even if the filename uses a different suffix.

### Regular file with parts

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

### File without parts

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

### Symlink style entry

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

## High-Level Flow

This diagram is the end-to-end control flow.
Step 7 is the data-model handoff where DXDA writes jobs into SQLite tables.
The next section explains those planning tables in detail.

```mermaid
flowchart TD
    S1[Step 1 Read manifest] --> S2[Step 2 Validate project file and folder fields]
    S2 --> S3{Step 3 Do all files have parts}

    S3 -->|Yes| S4[Step 4A Trusted path use manifest parts]
    S3 -->|No| S5[Step 4B Validated path call bulk describe]

    S5 --> S6{Step 5 Classify described objects}
    S6 -->|Regular| S7[Use platform parts from describe]
    S6 -->|Symlink| S8[No platform parts and drive present]

    S4 --> S9[Step 6 Build internal manifest objects]
    S7 --> S9
    S8 --> S9

    S9 --> S10{Step 7 Materialize SQLite download plan}
    S10 -->|Regular or symlink with parts| S11[Insert manifest_regular_stats one row per platform part]
    S11 --> S11A{Large regular part and chunking enabled}
    S11A -->|Yes| S11B[Set split_mode chunked and insert manifest_regular_chunk_stats rows]
    S11A -->|No| S11C[Set split_mode part_row]

    S10 -->|Symlink without parts| S12[Split by maxChunkSize then insert manifest_symlink_stats]
    S12 --> S12A[Insert one row in symlinks with whole file metadata]

    S11B --> S13[Step 8 Start workers]
    S11C --> S13
    S12A --> S13

    S13 --> W1[Step 9 URL workers prepare url and headers]
    S13 --> W2[Step 10 Download workers wait for URL ready jobs]
    W1 --> Q1[Queue jobsWithUrls]
    Q1 --> W2

    W2 --> S14[Step 11 Download ranged bytes and write to file offsets]
    S14 --> S15[Step 12 Update bytes_fetched and completion time in SQLite]
    S15 --> S16{Chunked regular part all chunk rows complete}
    S16 -->|Yes| S17[Run part-level integrity check on local bytes]
    S17 -->|Pass| S18[Mark parent regular part complete and integrity_done 1]
    S17 -->|Fail| S19[Reset parent row and all chunk rows for retry]
    S16 -->|No| S20[Continue]
```

## Step 7 Data Model: SQLite Tables and Entities

DXDA uses four planning tables with different responsibilities.

### 1) manifest_regular_stats

1. One row per platform part.
2. Stores per-part scheduling and progress.
3. Stores per-part integrity metadata for regular flow (`md5`, `checksum_type`, `checksum`).

### 2) manifest_regular_chunk_stats

1. One row per persisted chunk for large regular parts in `split_mode = chunked`.
2. Stores chunk `offset`, `size`, `bytes_fetched`, and `download_done_time`.
3. Used for parallel chunk download and resume of large regular parts.

### 3) manifest_symlink_stats

1. One row per local symlink chunk created by DXDA when platform parts are absent.
2. Stores chunk `offset`, `size`, `bytes_fetched`, and `download_done_time`.
3. Used for parallel chunk download and resume.

### 4) symlinks

1. One row per symlink file.
2. Stores whole-file metadata (`size`, `md5`) for final integrity check.
3. Linked to rows in `manifest_symlink_stats` through `file_id`.

### Entity Diagram (ERD)

```mermaid
erDiagram
    MANIFEST_REGULAR_STATS {
        string file_id
        string project
        string name
        string folder
        int part_id
        int64 offset
        int size
        int bytes_fetched
        int64 download_done_time
        string md5
        string checksum_type
        string checksum
        string split_mode
        int integrity_done
    }

    MANIFEST_REGULAR_CHUNK_STATS {
        string file_id
        string project
        string name
        string folder
        int part_id
        int chunk_id
        int64 offset
        int size
        int bytes_fetched
        int64 download_done_time
    }

    MANIFEST_SYMLINK_STATS {
        string file_id
        string project
        string name
        string folder
        int part_id
        int64 offset
        int size
        int bytes_fetched
        int64 download_done_time
    }

    SYMLINKS {
        string folder
        string id
        string proj_id
        string name
        int64 size
        string md5
    }

    MANIFEST_REGULAR_STATS ||--o{ MANIFEST_REGULAR_CHUNK_STATS : file_id_part_id
    SYMLINKS ||--o{ MANIFEST_SYMLINK_STATS : file_id
```

## Runtime Worker Flow

This zooms into Step 9 through Step 12.

```mermaid
flowchart TD
    A[Load incomplete rows from SQLite] --> B{Classify DB job type}

    B -->|DBPartRegular| R1[Prepare URL for regular job]
    B -->|DBPartRegularChunk| C1[Prepare URL for regular chunk job]
    B -->|DBPartSymlink| S1[Prepare URL for symlink job]

    R1 --> R2{Running in DX job and DX_DXDA_DOWNLOAD_URI is set}
    R2 -->|Yes| R3[Build URL from DX_DXDA_DOWNLOAD_URI and fileId/project then add X-Authorization]
    R2 -->|No| R4[Call file-id/download to get pre-authenticated download URL]

    S1 --> S2[Call file-id/download for symlink object]

    C1 --> C2{Running in DX job and DX_DXDA_DOWNLOAD_URI is set}
    C2 -->|Yes| C3[Build URL from DX_DXDA_DOWNLOAD_URI and fileId/project then add X-Authorization]
    C2 -->|No| C4[Call file-id/download to get pre-authenticated download URL]

    R3 --> Q[Enqueue URL ready job]
    R4 --> Q
    C3 --> Q
    C4 --> Q
    S2 --> Q

    Q --> D{Download worker consumes job}
    D -->|Regular split_mode part_row| DR[GET with Range\nmaxChunkSize is 16 MiB off platform or 64 MiB in DX job\nsub-ranges within one part are sequential in one worker\nwrite bytes and verify md5 when present]
    D -->|Regular split_mode chunked| DC[Acquire per-file in-flight permit\nGET one persisted chunk range\nwrite bytes]
    D -->|Symlink| DS[GET with Range\nmaxChunkSize is 16 MiB off platform or 64 MiB in DX job\neach DBPartSymlink row is one pre-split chunk job\nwrite bytes]

    DR --> U[Push completion event]
    DC --> U
    DS --> U
    U --> V[Update SQLite bytes_fetched and download_done_time]
    V --> F{Completed job is regular chunk}
    F -->|Yes| G[Check whether all chunk rows for part are complete]
    G -->|All complete| H[Run part-level integrity]
    H -->|Pass| I[Mark parent part complete and set integrity_done]
    H -->|Fail| J[Reset parent row and all chunk rows]
    F -->|No| K[Done]
```

## Sequence Diagram

This sequence is the same end-to-end path shown earlier, but expressed as interactions between components.
It reuses the same step numbering so you can map directly to the previous sections:

1. Step 1 to Step 6: manifest read, validation, and trusted or validated manifest construction.
2. Step 7: SQLite plan materialization, matching the table roles in Step 7 Data Model.
3. Step 8 to Step 10: worker startup and URL preparation, matching Runtime Worker Flow.
4. Step 11 to Step 12: ranged download, local writes, and SQLite progress updates.

```mermaid
sequenceDiagram
    participant CLI as dx-download-agent
    participant API as DNAnexus API
    participant DB as Local SQLite
    participant DL as Download URL
    participant FS as Local filesystem

    CLI->>CLI: Read manifest and validate format
    alt All files already have parts
        CLI->>CLI: genTrustedManifest
    else Some files are missing parts
        CLI->>API: POST system/findDataObjects
        API-->>CLI: describe output including parts and drive fields
        CLI->>CLI: makeValidatedManifest
    end

    CLI->>DB: Check whether manifest stats DB exists
    alt Stats DB does not exist
        CLI->>DB: CreateManifestDB
        alt Regular or symlink with parts
            CLI->>DB: Insert manifest_regular_stats rows
            opt Large regular part and chunking enabled
                CLI->>DB: Insert manifest_regular_chunk_stats rows
                CLI->>DB: Mark parent part split_mode=chunked
            end
        else Symlink without parts
            CLI->>DB: Split by maxChunkSize and insert manifest_symlink_stats rows
            CLI->>DB: Insert one row in symlinks
        end
    else Stats DB exists
        CLI->>DB: Validate schema and reuse existing progress rows
    end

    loop For each incomplete row
        alt Regular in DX job with DX_DXDA_DOWNLOAD_URI
            CLI->>CLI: Build URL locally from environment variables
        else Default
            CLI->>API: POST file-id/download with project and duration
            API-->>CLI: URL and headers
        end

        CLI->>DL: GET with Range and headers
        DL-->>CLI: bytes
        CLI->>FS: WriteAt offset
        opt Regular part in split_mode part_row
            CLI->>CLI: If part is larger than maxChunkSize, request sub-ranges sequentially in one worker
            CLI->>CLI: Verify md5 during download when md5 is present
        end
        opt Regular part in split_mode chunked
            CLI->>CLI: Each DB chunk row is an independent download job
            CLI->>CLI: Enforce per-file in-flight chunk cap
            CLI->>DB: Update manifest_regular_chunk_stats row on completion
            CLI->>CLI: When all chunk rows for part are complete, run part-level integrity
            alt Integrity pass
                CLI->>DB: Mark parent regular part complete and integrity_done=1
            else Integrity fail
                CLI->>DB: Reset parent row and all chunk rows for retry
            end
        end
        opt Symlink chunk row
            CLI->>CLI: One DB row is one pre-split chunk range
        end
        CLI->>DB: Update bytes_fetched and download_done_time
    end
```

## Notes

1. Symlink object with `parts`:
    - It does not go through the local symlink chunk-splitting path.
    - It is handled as part-based download and inserted into `manifest_regular_stats`.

2. Large regular part behavior:
    - If chunking is disabled, or part size does not cross the split threshold, it stays `split_mode = part_row`.
    - If chunking is enabled and part size crosses the threshold, it becomes `split_mode = chunked` and gets persisted chunk rows in `manifest_regular_chunk_stats`.

3. Symlink without `parts`:
    - DXDA pre-splits it into multiple rows in `manifest_symlink_stats`.
    - These chunk rows can be downloaded in parallel by multiple workers.

4. Resume after interruption:
    - DXDA reloads incomplete rows from SQLite.
    - It resumes from `bytes_fetched` and updates `download_done_time` on completion.
    - For chunked regular parts, parent completion is derived after all chunk rows finish and finalize integrity passes.

5. Different integrity checks by phase and type:
    - Download phase (regular): validates part MD5 when MD5 is present.
    - Download phase (chunked regular): validates full part after all chunk rows complete; on failure, parent and chunk rows are reset.
    - Inspect phase (regular): validates MD5 or checksumType/checksum metadata.
    - Inspect phase (symlink): validates whole-file MD5 from `symlinks`.

