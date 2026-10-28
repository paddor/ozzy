# Storage and recovery

This page owns Ozzy's on-disk formats, write barriers, retention, recovery, and
the supported failure model. Terms are defined in
[the overview](OVERVIEW.md#terms).

Both confirmation modes use one append-only segment journal per partition
per broker. Storage backends own file handles and physical jobs; no filesystem
call runs on a shard.

## Partition directory layout

Each partition store lives in
`<device-root>/topics/<topic>/partitions/<partition>/`.

| Path | Contents |
| --- | --- |
| `group.lock` | Exclusive ownership |
| `identity` | Group, broker, volume, store, and store generation |
| `CONFIGURATION` | Group configuration (at most 4096 bytes) or a recovery marker |
| `MANIFEST.<generation>` | Immutable selection of exact segment and checkpoint incarnations |
| `CURRENT` | Selects one manifest by generation and digest |
| `DURABLE` | Synchronized accepted-prefix evidence |
| `segments/<id>.log`, `segments/<id>.<generation>.log` | Operations and payloads; the second form is a replacement |
| `segments/spare.log` | The next new segment's file: a retired segment, or preallocated space |
| `indexes/<segment-id>-<segment-digest-hex>.idx` | Derived, rebuildable lookup |
| `checkpoints/<id-hex>/` | `manifest` plus `CHUNK.<8-hex-ordinal>` files |
| `staging/` | Unselected work (`index-*`, `.checkpoint-*.tmp`) |

Metadata temporaries are `.MANIFEST.<generation>.tmp`,
`.CURRENT.<generation>.tmp`, and `.DURABLE.tmp`.

| File | Magic | Bytes |
| --- | --- | --- |
| `identity` | `OZYIDENT` | 4096 |
| `CURRENT` | `OZYCUR\0\0` | 128 |
| `MANIFEST.<generation>` | `OZYMAN\0\0` | 384 plus 120 per segment |
| `DURABLE` | `OZYDUR\0\0` | Two 192-byte records, 64 KiB apart |
| Recovery marker in `CONFIGURATION` | `OZYRECOV` | 64 |

`identity`, `CURRENT`, and manifests use metadata version 2.

- Shard numbers and producer IDs never appear in paths or storage identity.
- Persisted incarnations stop a reused name from adopting an older partition.
- Formatting is explicit. A missing or corrupt file never triggers reformatting.
- Partitions keep independent histories and recovery. A shared device is still
  a shared failure and bandwidth domain.

## Volumes and exclusive ownership

- Each device root holds a checksummed `OZZY_VOLUME` marker binding cluster,
  broker, and volume IDs. Startup checks it before opening any journal.
- A missing or mismatched marker fails closed. Initialization never creates
  roots or replaces a marker, even a damaged one.
- `group.lock` cannot fence copies elsewhere; persisted placement selects the
  writable store. Opening an existing store cannot create even a missing lock.
- Metadata is written to a temporary, synchronized, then published. An
  interrupted temporary selects nothing; a retry recreates it. Existing final
  names must match exactly.
- Relocation is offline: stop the owner, verify and sync the copy, publish the
  placement, then retire the source.

## Integrity

- Checksums are domain-separated XXH3-128, seeded with XXH3-64 of the domain's
  UTF-8 bytes.
- A 32-byte digest slot holds the big-endian 16-byte result plus 16 zeros. The
  zeros add no strength. An object's own digest field hashes as zeros.
- Length and decoded-size bounds are checked before allocation. Framing, body
  digest, and chain must all validate.
- Checksums give no authentication, authority, or rollback protection.

## Segment layout

A segment is a 4096-byte header (`OZYSEG\0\0`, version 3), then physical write
groups, then unused capacity. Each group ends with padding and a 96-byte seal
(`OZYSEAL\0`); its size is a multiple of 4096. A group can hold several
canonical operations.

| Codec | Group contents before padding and seal |
| --- | --- |
| 0 raw, 1 per-operation LZ4 | Repeated 192-byte entry header (`OZJE`), encoded body, 8-byte alignment |
| 2 shared LZ4 | 64-byte shared header (`OZYBODY\0`), entry headers, one compressed body block, 8-byte alignment |

- The segment header binds group ID, segment ID, predecessor ID, the chain
  digest before the segment, capacity, and file generation (zero for the
  original file).
- Open checks every selected header. A manifest publication checks only the
  segments it adds or changes, or whose predecessor changed; the journal
  never rewrites a selected header or shortens its file.
- Entry headers stay uncompressed. They carry kind, operation identity, chain
  and body digests, lengths, and codec.
- A seal records group number, start and end offsets, entry count, a durable
  mark, and a group digest. The mark is the highest group number already
  durable when this group was prepared; the digest covers it.
- Group numbers rise through the log. A packed segment's groups can end
  before the next segment's first.
- Unknown versions, codecs, flags, or nonzero reserved bytes fail validation.
- Unused capacity holds zeros, preallocated space, or groups from an earlier
  use of the file, which never validate against the new header. Readers of a
  sealed segment read only its sealed bytes.

Exact layout: [segment codec](../ozzy-journal-segment/src/codec.rs).

### Compression

- Raw is the default. Segment LZ4 needs `BodyEncoding::Lz4` and the segment
  `lz4` feature, or `lz4-storage` on `ozzy` or `ozzy-runtime`.
- LZ4 is skipped when it saves too little or cannot shrink the aligned group.
  Groups of producer-compressed blocks stay raw.
- APPEND, segment, and OMQ transport compression are independent. All keep
  record boundaries, retry identity, and confirmation policy.
- Reads can pass whole producer blocks through; partial reads decode only the
  selected extent.

## Canonical operations

| Kind | Operation | Kind | Operation |
| ---: | --- | ---: | --- |
| 1 | `CreatePartition` | 8 | `Barrier` |
| 2 | `OpenProducer` | 9 | `ProducerResultFloor` |
| 3 | `Append` | 10 | `ResolvedAppend` |
| 4 | `Progress` | 11 | `DedupWatermark` |
| 5 | `Assign` | 12 | `GroupPlan` |
| 6 | `Trim` | 13 | `GroupDelete` |
| 7 | `PartitionPolicy` | 14 | `RetireProducer` |
| | | 15 | `Schedule` |
| | | 16 | `Release` |
| | | 17 | `Carry` |

Integers are big-endian, IDs 16 bytes, counts and text lengths `u32`. Exact
schema: [operation codec](../ozzy-journal/src/operation/codec/).

### APPEND body

Batch count `u32`, then complete batches: a 76-byte header, descriptors, then
payloads.

| Batch header field, in order | Bytes |
| --- | ---: |
| Partition incarnation | 16 |
| Owner epoch | 8 |
| Producer ID | 16 |
| Producer epoch | 8 |
| First sequence | 8 |
| First partition offset | 8 |
| Append timestamp, Unix milliseconds | 8 |
| Record count; high bit compact lengths, next bit encoded payload | 4 |

Raw single-part payloads of at most 255 bytes use contiguous message IDs
(`16*N` bytes), lengths (`N` bytes), then payloads. Zero length means one empty
part. General descriptors use:

| Field | Bytes |
| --- | ---: |
| Message ID | 16 |
| Tagged part count; high byte codec 0/raw or 1/LZ4 | 4 |
| LZ4 only: original payload bytes | 4 |
| Part lengths | 4/part |

- Sequence and offset equal the batch's first values plus the record index.
- Producer LZ4 batches use clear general descriptors, codec `u8`, decoded and
  encoded lengths (`u32` each), and the exact producer block.

### Other bodies

- `ResolvedAppend` (kind 10): exactly one batch, then logical time `u64`,
  attempt count `u32`, run count `u32`, outcome runs, and one 32-byte BLAKE3
  fingerprint per duplicate. A run is disposition `u8` (0 stored, 1
  duplicate), first offset `u64`, and count `u32`.
- Stored outcomes consume offsets; every attempt consumes a producer sequence.
  An all-duplicate batch has record count zero and no descriptors or payload.
  Ordinary APPEND batches must be nonempty.
- `DedupWatermark` (kind 11): partition incarnation, owner epoch `u64`, logical
  milliseconds `u64`.
- `GroupPlan` (kind 12): format `u8` (1), coordinator and group incarnations,
  name, expected and new revision, creation selector (`u8` tag, `u64`), members
  sorted by ID (member, node, join ID), one target per topic partition in
  numeric order (partition, last changing revision, desired member, prior
  member; each member optional), operation ID.
- `GroupDelete` (kind 13): format `u8` (1), coordinator and group incarnations,
  the committed revision it retires, operation ID.
- `CreatePartition` stores the dedup window, then the maximum delay, as
  `u64` milliseconds; zero disables each. A partition cannot enable both.
- `RetireProducer` (kind 14): partition incarnation, producer ID, epoch
  `u64`, next sequence `u64`. It has no operation ID: the epoch and next
  sequence fence it against any open or append ordered first.
- `Schedule` (kind 15): exactly one nonempty producer batch with first
  offset zero and the acceptance timestamp, then first schedule sequence
  `u64`, then one due time `u64` (Unix milliseconds) per record. It
  consumes producer and schedule sequences but no offsets.
- `Release` (kind 16): exactly one nonempty batch with zero producer fields,
  the next offsets, and the release timestamp, then the release key of its
  last record: time `u64`, schedule sequence `u64`.
- `Carry` (kind 17): exactly one batch, possibly empty, with zero producer
  fields, first offset zero, and the carry timestamp; then the carry floor
  before and after it, each an operation `u64` and record index `u32`;
  then one release key per record. It consumes no offsets or sequences.

### Scheduled records

- A held record's release key is `(max(due, acceptance), schedule
  sequence)`. Every schedule or release timestamp is at least the
  partition's latest one, so a new key sorts after every released key.
- Partition state holds only the last released key, the carry floor, and
  the count and encoded bytes (descriptor, part lengths, payload) of held
  records. Payloads stay in the `Schedule` operation, or the `Carry` that
  last copied them, until release.
- A `Carry` copies held records from the carry floor on, in position order,
  with their keys, and moves the floor to its end. Every held record before
  the floor was released or carried. Partition state checks the floor, a
  timestamp of at least the partition's latest one, keys past the last
  release with issued schedule sequences, an end no later than the `Carry`
  itself, and record and byte counts within the held totals. Totals do not
  change.
- Each copy keeps a held index of the rest: key, `Schedule` or `Carry`
  operation, record index, encoded bytes, and message ID. Rows before the
  carry floor are skipped; a carried record's row moves to its `Carry`.
  Rows of the active segment
  and later operations stay in memory. Once 262144 are, a copy rolls its
  segment before its next write when it has room to roll, so they move to
  its index file. Of sealed segments, only the next rows after the last
  release are loaded from their index files, 65536 by default. A row's
  place is certain once every row before it is loaded, and the primary
  releases only certain rows.
- A `Release` copies exactly the held records through its key, in key order.
  Each copy checks message IDs and encoded byte counts against its held
  index and the `Schedule` operations before it in the same batch, first
  loading sealed rows the release passes. Validation of that batch waits for
  those reads. Segments differ between copies, so a release can pass more
  sealed rows than the window; they stay loaded until it applies. Partition
  state then checks the totals, a key past the last release, a timestamp
  that reaches the key's time, and the next offsets. A repeated release does
  not pass the last one and fails.
- Each copy checks a `Carry` against the held rows in its range, read from
  index files and memory: keys, message IDs, and encoded byte counts, in
  position order, after releases and with schedules and carries before it
  in the same batch. Validation of that batch waits for the read.
- A scheduled attempt's retry result names its `Schedule` operation and maps
  the sequence to its schedule sequence.
- Retention keeps the oldest segment that reaches the committed carry floor
  and whose last scheduled-record key is past the last committed release,
  and every later segment. A segment wholly before the floor holds no record;
  one the floor splits stays. Checkpoints do not
  hold held records, so retirement checks the same segment floor. A
  `Schedule` acceptance time counts toward its segment's age, so retries of a
  released schedule resolve for the retention age. A `Carry` timestamp
  counts too, so carried records stay for the age.
- Readers and indexes treat a `Release` batch like an APPEND batch.
  `Schedule` and `Carry` records have no offsets; indexes list them by
  release key only.
- Scheduled payloads are raw, so a release can copy any prefix of held
  records without recompressing.

## Record identity and deduplication

- Producers supply message IDs, part boundaries, and bytes. Brokers assign
  offsets, append timestamps, operation numbers, and digests.
- Producer epochs and sequences are per partition; producers share offsets.
- Exact sequence-to-offset retry ranges survive checkpoints and leader changes.
  Ranges merge only when both sequences and offsets are adjacent. Expired retry
  results fail explicitly.
- At most 1,048,576 ranges are kept across a journal's partitions, and
  checkpoints fit them all. At that limit a new range drops the oldest range
  of the same producer and raises its retry floor; a producer without one is
  refused.
- A leader retires sessions idle for `producer_idle_secs` with their ranges.
  Idleness is the leader's own observation: a new leader starts every session
  with a full expiry. The partition keeps the highest retired epoch, and a
  recreated session starts above it.
- Deduplication is a per-topic option over nonzero message IDs. The first
  record wins within the window, which is fixed at partition creation.
- A duplicate stores only its outcome and a BLAKE3 fingerprint of message ID,
  encoding, part count, part lengths, and bytes. It never refreshes the ID.
  Checkpoints keep its reference to that fingerprint operation and its logical
  acceptance time.
- A retry of an accepted, unconfirmed original waits until the original is
  applied. That producer's later requests stay in order behind it.
- A full index holds new unique requests until expiry frees room. Live
  identities are never evicted.
- Expiry follows committed logical time only. A discarded `DedupWatermark`
  changes nothing. Followers apply confirmed time before admitting new IDs.
- Both modes keep live IDs after their payloads retire.

## Consumer progress and groups

Commands are in [the protocol](PROTOCOL.md). Stored state:

- `Progress` stores the exclusive next offset, which may equal the record end.
  Replicas validate it against the records ordered before it. Leaders admit
  only cursors over confirmed records.
- Group progress carries the assignment epoch and member ID. Revoked or
  mismatched assignments cannot move the cursor.
- `Assign` persists the initial cursor before any acknowledgment. Checkpoints
  preserve assignment fences.
- Retention never replaces a group's cursor. An expired resume cursor returns
  `HistoryExpired`, with no reset to the floor.
- A `GroupPlan` revision supersedes the previous image even if not every source
  confirmed it; sources fence older revisions.
- `GroupDelete` applies only at exactly its revision and frees the name. Sources
  are first revoked by an `Assign` with no member at revision `u64::MAX`.

## Writes, barriers, and roll

1. Validate authority, retry identity, and limits; assign offsets; freeze bytes.
2. Write physical groups; resume short or interrupted writes until complete.
3. Install completions in order for the matching generation. An ambiguous
   failure fences the writer.
4. Satisfy the policy, then apply and reply. `DURABLE` evidence follows on its
   own; no vote or confirmation waits for it.

Segment writes use `O_DSYNC` in both modes. Each completed write is durable
and confirms or votes at once.

With the `aio` backend (the Linux default), segment data writes use a separate
`O_DIRECT` handle. Headers, metadata, and reads stay buffered.

- A write group takes the ready operations; there is no fill delay.
- Preallocation reserves the full capacity; appends advance a logical prefix.
- The first write into an unwritten extent costs a metadata commit; on XFS a
  4 KiB `O_DSYNC` write took 1.6 ms there and 0.65 ms into written blocks. So
  every new segment, from a roll or from a log a new view installs, takes the
  spare when there is one.
- A job beside writes allocates the spare at full capacity: at open and
  after each roll, installation, and cleanup. A roll waits for it, so it
  needs no new space.
- `format` writes up to 128 MiB of zeros once where a new store's first
  appends land: the first segment of a local store, or a spare in a
  replicated store, whose first election installs its log into a new segment.
- `O_DSYNC` is not multi-write atomicity. AIO completion alone is no barrier.
  Metadata files and directories need their own syncs.
- Cancellation releases observation only. Buffers, handles, and admission stay
  held until the physical job completes.

### Segment roll

1. Create the successor with a new ID, move the allocated spare over it, and
   sync it.
2. Write and sync its header, which binds the predecessor ID and the chain
   digest before the successor.
3. Sync the predecessor and the `segments/` directory.
4. Publish the new manifest, then `CURRENT`. The manifest also commits the
   owner's applied prefix when that is newer than its commit.

Selected files are immutable. Replacement uses a fresh `<id>.<generation>.log`.
An unselected successor is a leftover for cleanup. Reads keep their original
generation.

## Durable recovery evidence

- Every store has `DURABLE`. It holds the synchronized accepted prefix,
  including operations without a commit announcement. It is published after
  the writes it covers, off the confirmation path; until then, later group
  seals mark those writes durable.
- A partition publishes it after 20 ms with no write in flight, after half a
  window of writes, when maintenance is due, and before elections and
  shutdown. Under load it no longer competes with every write for the disk.
- Each copy sits in one 512-byte sector. A publication rewrites both in place,
  older first, under one data barrier and without rename. Sector writes are
  assumed atomic.
- Evidence binds group, store, store generation, configuration epoch, manifest
  generation, active segment, accepted position, broker, and volume.
- Recovery takes the newest intact in-scope copy and repairs the other before
  writes. Two damaged copies fail startup.
- A changed selected manifest fences an in-flight publication. Recovery never
  falls back to an older manifest or temporary.
- Authorized replacement with a new active segment ID supersedes older evidence
  under protected-history rules.

## Recovery

| Condition | Action |
| --- | --- |
| Normal open | Verify identities, authority, selected files, seals, chain, and anchors |
| Zero or stale suffix | Unused capacity |
| Active damage after durable prefix | End the log at the damaged group; later groups of this use lose their seals; restore allocation |
| Damage at/before durable prefix, or marked durable by a later valid group | Refuse without changing bytes |
| Sealed/installed damage | Refuse strict open; authorized sealed repair may apply |
| Corrupt or missing index | Rebuild from validated segment bytes |
| Corrupt authority / lost store | Fail closed or stay nonvoting for recovery |

- Writes in flight can complete out of order. A valid group after a damaged
  one that does not mark it durable followed an unfinished write; recovery
  discards both. Overwriting the later group's seal stops a byte-identical
  rewrite of the damaged group from chaining into it again.
- Replication authorizes suffix replacement; see [replication](REPLICATION.md).
  Protected committed fragments survive physical regrouping. A request may
  keep a longer verified prefix, never past its commit; its exact position
  must be in local history.
- Replacements are staged, validated, synchronized, and selected atomically.
  The old selection stays authoritative until then. Shutdown aborts unpublished
  work without lowering a durable promise.
- Quarantine replaces `CONFIGURATION` with the `OZYRECOV` marker, removing
  voting eligibility. Old files stay for inspection until normal cleanup.
- Recovery transfers checkpoint state, required retained operations, and the
  full accepted suffix. Schema, digest, anchor, chain, and destination
  publication all validate before voting.

### Repairing sealed segments

- Sealed repair is copy-on-write. It keeps valid identity, configuration,
  selection, and durability evidence.
- Fresh authority from both other brokers allows a bounded inventory and
  missing-range fetches from one frozen donor.
- Validated local operations may be reused; replacement keeps the original
  canonical boundaries.
- New generations are synchronized, selected, and replayed privately before a
  fenced restart or election. Damaged originals stay until safe cleanup.
- Active damage or an unavailable range needs full recovery or refusal.

## Indexes, checkpoints, and retention

### Indexes and reads

Index files bind one sealed source by name. Header: 4096 bytes, `OZYIDX01`,
format 4. Rows: record offsets 96 bytes, message IDs 80, operation IDs 80, a
dense 56-byte numeric selector for every canonical operation, including
payload-free duplicate results, and 64-byte scheduled records.

- Scheduled-record rows sort by partition, release key, then operation and
  record index, since a carried copy keeps its original's key. Each holds
  the key, its `Schedule` or `Carry` operation, record index, encoded bytes,
  and message ID, so held records after a release key can be listed without
  reading payloads.

- A missing or corrupt index is rebuilt from validated segment bytes. A
  background job builds each sealed segment's index beside later writes; a
  build validates the segment once.
- Identity lookups keep each sealed index's operation IDs resident (up to
  64Ki IDs), so a miss skips reading that index.
- Numeric lookup revalidates the operation's framing and digest before use.
- Time seeks use APPEND-head timestamps. ID seeks search retained segments and
  need an explicit duplicate policy.
- An older durable source can be read while later writes are unsynchronized,
  without exposing them or advancing durable evidence.
- Captured reads and donor snapshots delay deletion; subscription cursors do
  not. Read caches are bounded by the shard's `budget.resident_bytes` share.

### Checkpoints

`checkpoints/<id>/manifest` has a 4096-byte header (`OZYCHK01`, version 2) and
64-byte chunk descriptors; `CHUNK.<ordinal>` files hold the image.

Canonical checkpoint images start with a 104-byte `OZYCIMG\0` header: version
`u16` (1), six zero bytes, core snapshot length `u64`, partition incarnation
(16 bytes), and the 64-byte dedup index header. The last 80 bytes are zero
without an identity section. The core snapshot (`OZYSTA01`, version 6,
256-byte header) follows, then identity rows. Bare snapshots and other versions
are rejected. Coordinator group images are each a `u32` length plus the
`GroupPlan` body.

The dedup index header is 64 bytes: `OZYDIDX\0`, version `u16` (1), six zero
bytes, then six `u64` values: canonical operation, logical time, window, first
live offset, exclusive record end, and row count. Each 32-byte row is message
ID, offset, and logical acceptance time. The enclosing checkpoint supplies
integrity and partition authority.

- Checkpoints hold state, producer coordinates, retry floors, live confirmed
  identities, the last released key, carry floor, and held-record totals,
  and the original committed operation and digest. Never payloads.
- Failed or canceled builds never select themselves. Installation checks the
  captured position and source lineage.
- Decoded state stays private until every chunk and the state digest match.
- Restore checks policy, offset order, unique IDs, and the window. An invalid
  or incomplete identity image supplies no admission state.
- Recovery restores the matching identity image before admission. Canonical
  state alone cannot authorize an empty index.
- Received checkpoints stage privately; limits apply before allocation.
  Unselected input authorizes neither voting nor admission.

### Retention

Topics set `retention.max_age_secs` and `retention.max_bytes`; see
[`ozzy-config`](../ozzy-config/README.md).

- Retention removes whole sealed segments, oldest first. An aged active segment
  rolls first.
- Age uses each segment's newest append time, so backward clocks cannot expire
  newer records early.
- Byte targets count active and sealed capacities per partition per broker.
- Order: confirm `ProducerResultFloor` and `Trim` operations, sync and select
  a checkpoint, publish a manifest without the prefix, then delete files.
  Payloads and duplicate proof operations go only after their producer result
  floors confirm.
- Retention debt runs in bounded steps between foreground turns. The periodic
  interval applies only after catching up.
- A due turn waits for accepted operations to apply. New proposals wait with
  it; replay, repair, and recovery donor work go on, since the operations may
  commit only through a lagging or recovering copy.
- A view change abandons unfinished retirement; its files become unselected
  leftovers.
- Policy changes become canonical `PartitionPolicy` operations; confirmation
  waits for application.
- Retirement never changes application state. Surviving predecessor digests
  are never renumbered. Expired readers receive explicit gaps.
- After a crash, a retry whose confirmation was lost resolves to its original
  offset from the checkpoint plus the replayed suffix.
- Cleanup reclaims unselected segments, indexes, manifests, temporaries, and
  checkpoints in bounded batches. It never removes a selected or pinned object
  and changes no record or retry floor. A background job removes only objects
  older than its captured selection, and only while `CURRENT` names that
  generation or a newer one. A failed cleanup job fences the owner.
- Cleanup renames one unselected segment file to `segments/spare.log` instead
  of unlinking it, unless a spare with written bytes exists. It replaces a
  preallocated spare, which has none. So a byte-limited partition can use one
  segment more than `max_bytes` on disk.
- Opening a store removes recognized `staging/` leftovers.

### Full partitions

A partition journal holds at most 256 segments of `segment_bytes`.

- With less than a sixteenth of that space left, the partition refuses new
  records with NACK 10 (retry after backoff). With less than a sixty-fourth
  left, it also refuses producer opens, consumer progress, and assignments.
- Records and control requests also leave room to release every held
  scheduled record: its encoded bytes plus a `Release` frame, entry, and write
  group of its own, and one write group per segment the releases fill. A
  `Release` needs only the sixty-fourth, so held records can always leave and
  end their retention hold. A `Carry` needs the sixty-fourth and that room,
  since its sources stay until retention frees them.
- Trims and policy changes always pass, so retention can still free space.
- A retention byte target covers at most 224 segments, so retention that lags
  a few segments still frees space before refusals start.
- Followers group the same operations on their own timing and can need more
  space than the leader. Packing gives that space back. A replica whose
  journal fills anyway holds its writes until a pack or a retirement frees a
  segment, instead of failing.

### Full devices

A spare the device has no room for (`ENOSPC`, `EDQUOT`) is no fault.

- Without an allocated spare, the partition refuses new records once less than
  a sixteenth of its active segment is left, and producer opens, consumer
  progress, and assignments below a sixty-fourth. New work must leave that room
  free, beside the segment limit reserves. Trims and policy changes still
  pass.
- Writes that reach the end of the active segment hold until the spare is
  allocated. Followers hold the same way.
- Allocation retries a second after the failure, or at once after cleanup.
- Other writes that need new space, such as checkpoints, still fence the
  owner on `ENOSPC`.

### Packing

A replica with at most 24 free segments packs sparse sealed segments.

- It picks the run of adjacent sealed segments that frees the most segments
  per byte rewritten. The run must fit one segment and free at least what it
  rewrites.
- A job beside writes measures the run, then rewrites its operations into
  groups of at most 8 MiB. It reads and writes at most 8 MiB at a time. One
  manifest publication between writes selects the packed segment in place of
  the run.
- The packed segment is a new file generation of the run's last segment ID,
  so the next segment's header still names its predecessor. Its groups number
  from the run's first.
- Operations, digests, and the chain do not change. A crash before the
  publication leaves an unselected file for cleanup.
- A pack starts and installs only while no retirement or cleanup runs, and
  retirement waits for a pack. A view change abandons an unfinished pack.
- Suffix installation at the segment limit packs the best sealed run of its
  new selection, kept or staged, before it rolls. Reads and groups stay
  within the staging bounds. A replaced kept segment stays selected until
  publication and is cleaned up after; a replaced staged one is removed at
  once.

## Operator tuning

Defaults live in [`ozzy-config`](../ozzy-config/README.md).

| Key | Tradeoff |
| --- | --- |
| `topics.<t>.segment_bytes` | Fewer rolls and syncs versus coarser retention, RAM, and recovery work |
| `topics.<t>.max_append_bytes` | Largest APPEND (at most 8 MiB); record, operation, and segment limits must all fit it |
| `devices.<d>.workers.backend` | `aio`: Linux AIO with `O_DIRECT` data writes; `pool`: blocking workers |
| `workers.aio_depth` | Kernel-outstanding direct writes |
| `workers.max_inflight`, `queued_jobs`, `queued_bytes` | Ordinary device jobs: throughput versus RAM |
| `workers.progress_jobs`, `progress_bytes` | Capacity reserved for progress jobs |
| Shard `budget` | Backlog of accepted work: burst and stall tolerance versus RAM |

- A segment rolls at its physical or decoded limit, whichever comes first,
  or early at its held-record bound.
- Backlog must cover write and roll stalls. It cannot fix a sustained disk
  deficit. Transport aliases, reads, and dirty pages need additional RAM.

Embedding runtimes can enable replica maintenance; all are off by default:

| Builder | Work |
| --- | --- |
| `with_storage_validation` | Cooperative segment checks; default steps 256 KiB / 2 ms |
| `with_metadata_cleanup` | Old manifest removal, bounded per turn; deletes no payloads |
| `with_orphan_cleanup` | Unselected artifact removal with selection and pin recheck; advances no floor, skips active staging |

See the [calibration commands](../ozzy-bench/README.md#disk-calibration).

## Supported failure model

- Supported: Linux local filesystems and process crashes. Other targets refuse
  `O_DSYNC` and `O_DIRECT` opens.
- Barriers rely on filesystem and device honesty and on atomic sector writes.
- `SIGKILL` keeps the OS page cache; it does not model power loss.
- Surviving group authority can permit selected repairs.
- A valid-looking rollback of authority metadata or of a whole store is outside
  the supported model.

## In the code

- `ozzy-journal-segment/src/`: segment codec, metadata, `DURABLE`, indexes,
  checkpoints, recovery.
- `ozzy-journal/src/operation/`: canonical operation bodies.
- `ozzy-core/src/checkpoint.rs`, `dedup/`, `state/snapshot.rs`: checkpoint
  image sections.
- `ozzy-runtime/src/replica_journal/owned/retention/`: retention and cleanup.
- `ozzy-io`, `ozzy-io-aio`, `ozzy-io-pool`: file jobs and backends.
