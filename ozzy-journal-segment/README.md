# ozzy-journal-segment

Append-only segment journal for [Ozzy](https://github.com/paddor/ozzy)
partitions. Applications use the
[`ozzy`](https://github.com/paddor/ozzy/blob/main/ozzy/README.md) SDK.

## Owns

- Segment, index, manifest, and checkpoint file formats.
- Integrity checks: physical framing, canonical operations, chain digests,
  manifests, checkpoints, and recovery evidence.
- Active writes with an explicit durable or buffered mode.
- Recovery, suffix installation, and sealed-segment repair.
- Whole-segment retention and cleanup of unreferenced files.

File work runs on async storage backends; there is no blocking path. The
partition actor owns journal state and installs completed results. A writer
reply follows the configured confirmation boundary, never a transport receipt.

## Entry points

| Item | Use |
| --- | --- |
| `GroupJournal` | One partition's journal: append, sync, replay, close |
| `RecoveryDirectory` | Locked partition directory metadata for opening and recovery |
| `PartitionRead` | Captured read that holds back deletion of its segments |
| `SegmentWriter`, `SegmentWriteMode` | Active segment writes |
| `encode_segment_header`, `scan_segment`, `encode_manifest`, ... | Exact on-disk codecs |

Feature `lz4` reads and writes LZ4 segment bodies.

## Docs

- [Storage](https://github.com/paddor/ozzy/blob/main/doc/STORAGE.md): formats,
  barriers, retention, and recovery.
- [Runtime](https://github.com/paddor/ozzy/blob/main/doc/RUNTIME.md): storage
  owners and cancellation.
