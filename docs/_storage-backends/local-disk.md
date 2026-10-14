---
title: Local Disk
layout: default
nav_order: 2
---

# Local disk storage

Tusd can store uploads on the local disk in a specific directory. This storage backend is the default if no other configuration flags are supplied. The `-upload-dir` flag specifies the directory that will be used. If this directory does not exist, tusd will create it. For example:

```sh
$ tusd -upload-dir=./uploads
```

When a new upload is created, for example with the upload ID `abcdef123`, tusd creates two files:

- `./uploads/abcdef123` to hold the file content that is uploaded
- `./uploads/abcdef123.info` to hold [meta information about the upload]({{ site.baseurl }}/storage-backends/overview/#storage-format)

## Custom storage location

The locations of the two files mentioned above can be fully customized using the [pre-create hook]({{ site.baseurl }}/advanced-topics/hooks/). The location of the `.info` file is derived from the upload ID, which can be customized by the pre-create hook using the [`ChangeFileInfo.ID` setting]({{ site.baseurl }}/advanced-topics/hooks/#hook-requests-and-responses). Similarly, the location where the file content is saved is by default derived from the upload ID, but can be fully customized using the [`ChangeFileInfo.Storage.Path` setting]({{ site.baseurl }}/advanced-topics/hooks/#hook-requests-and-responses).

For example, if the pre-create hook returns the following hook response, an upload with ID `project-123/abc` is created, the info file is saved at `./uploads/project-123/abc.info`, and the file content is saved at `./uploads/project-123/abc/presentation.pdf`:

```js
{
    "ChangeFileInfo": {
        "ID": "project-123/abc",
        "Storage": {
            "Path": "project-123/abc/presentation.pdf"
        }
    },
}
```

If the defined path is relative, it will be resolved from the directory defined using `-dir`.

## Content deduplication

When the same large file is uploaded repeatedly, every upload normally occupies its own copy on disk. The `tusd-dedupe` command merges the binary files of completed uploads with identical content into a single physical copy using hard links:

```sh
$ tusd-dedupe -dir=./uploads
```

The shared copies live in a hidden content-addressed directory (`./uploads/.blobs` by default, named by SHA-256). Uploads keep their own paths and continue to read their own data; the filesystem link count acts as the reference count, so terminating one upload never removes data that other uploads still reference.

The command is designed to be run repeatedly, for example as a cron job:

- Only **completed** uploads (declared size known and matching the bytes on disk) are considered. In-progress uploads are never touched.
- Identity is verified by hashing the full file content; the upload path is switched atomically, so a failure or a crashed run never leaves an upload reading empty or missing data. Stale temporary links from an interrupted run are cleaned up automatically at the start of the next run.
- It can run while tusd is serving the same directory. When tusd uses the file locker, it acquires the same per-upload locks, and uploads that are busy are skipped for that run and picked up on the next one. Only one reconciliation runs at a time.
- On each run, blobs no longer referenced by any upload are garbage collected.

Useful flags:

```sh
# Report what would be done without modifying anything
$ tusd-dedupe -dir=./uploads -dry-run

# Keep unreferenced blobs (disables garbage collection)
$ tusd-dedupe -dir=./uploads -no-gc

# Skip busy uploads faster instead of waiting up to 30s
$ tusd-dedupe -dir=./uploads -lock-timeout=5s -verbose
```

As with the file locks, hard links are required, so the upload directory must reside on a local filesystem that supports them (see the NFS notes below). Uploads whose binary was customized to a location outside the upload directory are skipped. If you back up the upload directory, use a backup tool that preserves hard links so the deduplicated copies are not expanded again on restore.

## Issues with NFS and shared folders

Tusd maintains [upload locks]({{ site.baseurl }}/advanced-topics/locks/) on disk to ensure exclusive access to uploads and prevent data corruption. These disk-based locks utilize hard links, which might not be supported by older NFS versions or when a folder is shared in a VM using VirtualBox or Vagrant. In these cases, you might get errors like this:

```
TemporaryErrors (Lockfile created, but doesn't exist)
```

We recommend you to ensure that your file system supports hard links, use a different file system, or use one of tusd's cloud storage backends. If the problem still persists, please open a bug report.
