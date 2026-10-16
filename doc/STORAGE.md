# Storage and recovery

All three confirmation modes use independent append-only segment journals per
partition. Actors own journal state; asynchronous backends own file handles,
physical jobs, and fixed device workers. No filesystem calls run on shards.

## Partition directory layout

`<broker-root>/topics/<topic>/partitions/<partition>/` contains one store.
Shard numbers and producer IDs are absent from paths/storage identity. Persisted
incarnations prevent name reuse from adopting an older partition.

| File / directory | Contract |
| --- | --- |
| Lock, identity, `CONFIGURATION` | Exclusive ownership, store/group/policy binding |
| `MANIFEST.<generation>`, `CURRENT` | Immutable selection of exact physical incarnations |
| `DURABLE` | Synchronized accepted-prefix evidence for disk groups |
| `MEMORY_VOTING` | Running/clean-stop evidence for RP |
| `segments/` | Canonical metadata operations and payloads |
| `indexes/` | Rebuildable record lookup |
| `checkpoints/` | Canonical state and original chain anchor |
| `staging/` | Unselected replacement work |

Formatting is explicit; missing/corrupt files never trigger reformatting.
Partition histories, operations, and recovery stay independent despite shared
devices. Device failure and bandwidth remain shared failure/resource domains.

## Record identity

Producers supply message IDs, multipart boundaries, and bytes. Brokers assign
partition offsets, append timestamps, canonical operation numbers, and digests.
Producer sequences/epochs are independent per partition; interleaved producers
share offsets. Exact sequence-to-offset retry ranges survive checkpoints and
leader changes. Ranges merge only when both sequences and offsets are adjacent.
Expired results fail explicitly. Producer retries use epochs and sequences;
optional topic deduplication uses nonzero message IDs within its fixed window.

Resolved attempts keep stored/duplicate dispositions separately from record
offsets. Duplicate retries reference their fingerprint operation; checkpoints
preserve that reference and logical acceptance time. Retention confirms producer
result floors before deleting either payload bytes or duplicate proof operations.

All-unique proposals preserve the SDK's compressed block; mixed proposals decode
once, retain survivors, and adaptively repack them. All-duplicate proposals store
only outcomes and retry fingerprints. An accepted-only original defers the bounded
request until application proves its confirmation; the owner then reclassifies it.
Other producers keep admitting meanwhile. That producer's later requests queue
behind the deferred one, so its sequences still validate in arrival order.
Unadmitted validation retains the complete input, including overlapping retry
prefixes, so cancellation or a stale image cannot consume identities or sequences.

`DedupWatermark` (kind 11) advances idle logical time without records: partition
incarnation (16 bytes), owner epoch, and logical milliseconds. Integers are
big-endian `u64`. Internal watermarks use their canonical operation number and
strictly increasing time, with no client retry identity or control-index claim.
The owner fence must match and time
must advance. Only canonical commitment makes expiry eligible; discarded
accepted watermarks change no confirmed dedup state.

A full identity index holds unique requests in their existing bounded arenas.
Idle maintenance continues while those requests wait, proposing a watermark only
when the oldest confirmed identity reaches its window. Once a retention turn
finds expirable identities, further bounded expiry steps run between foreground
turns without waiting for a settled journal; record retention and checkpoint
retirement still wait for one. A settled turn runs one expiry step, then record
retention, so continuous expiry cannot starve it. Bounded cleanup and
confirmation progress trigger reclassification; live identities are never evicted.
Followers apply confirmed logical time before admitting replacement identities.
A follower that runs out of identity capacity while it still holds accepted,
unapplied operations defers validating its staged suffix until it applies more:
the leader freed that capacity at a commit the follower has not applied or not
yet learned of. With nothing left to apply, the error still fails the partition.

Canonical `Progress` stores the exclusive next processing offset. It may equal
the end, including zero for an empty partition. Every replica validates it, and
an initial `Assign` cursor, against the records ordered before it, never against
its own commit watermark; a commit covering the operation covers those records.
Leaders admit only cursors over confirmed records. Checkpoints preserve
assignment fences.
Group progress carries both the source assignment epoch and its member ID;
revoked assignments and mismatched members cannot advance the cursor.
`Assign` persists the initial cursor before any acknowledgment. Handoffs capture
the exact accepted progress and advance both the source epoch and coordinator
plan revision. The source allocates the epoch and fills the expected fence and
captured cursor from its accepted image; clients supply only the plan revision,
member, and the group's creation selector. Later progress leaves the captured
start unchanged. Retention never replaces an existing group's cursor, even before
its first acknowledgment. Covered updates wait for that prefix to confirm; queries
expose only confirmed progress. An expired resume cursor returns
`HistoryExpired`, with no reset to retention.

`GroupPlan` (kind 12) persists one coordinator image on the group's coordinator
partition: coordinator and group incarnations, name, expected and new revision,
creation selector, members sorted by ID (member, node, join identity), and one
target per topic partition in numeric order (partition, last changing revision,
desired member, prior member). A revision supersedes the previous image whether
or not every source confirmed it; sources fence older revisions themselves.
Checkpoints carry each image as its canonical body, prefixed by a `u32` length.
`GroupDelete` (kind 13) retires one image: format `u8=1`, coordinator and group
incarnations, the committed revision it retires, and an operation ID. It applies
only at exactly that revision, removes the image and its member and source
counts, and frees the name; a later plan with that name has a new incarnation.
Sources are revoked first with an `Assign` naming no member at plan revision
`u64::MAX`, so the retired incarnation keeps one fenced assignment per source.

### Canonical APPEND body

All integers are big-endian. A body starts with batch count `u32`, followed by
complete batches: 76-byte header, descriptors, then payloads.

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

Sequence/offset equals the batch's first value plus record index. Producer LZ4
batches use clear general descriptors, codec `u8`, decoded/encoded lengths
(`u32` each), and the exact producer block. They retain no decoded copy.
Partial reads/retries decode lazily; fresh suffixes are rebuilt without changing
retained identities. Stale validation returns to intact SDK retry bytes.

Validated body proofs retain record/part/decoded-byte totals. Live admission and
storage reuse those immutable proofs; recovery validates physical bytes anew.
Exact schema: [operation codec](../ozzy-journal/src/operation.rs).

`ResolvedAppend` (kind 10) contains exactly one batch, then logical time `u64`,
attempt count `u32`, run count `u32`, 13-byte outcome runs, and one 32-byte
BLAKE3 fingerprint per duplicate. Stored outcomes consume offsets; every attempt
consumes a producer sequence. An all-duplicate batch has a zero record count and
no descriptors or payload. Ordinary APPEND still requires nonempty batches.

## Segment layout

A 4096-byte header precedes physical write groups and unused preallocated capacity.
Each group ends with padding and a 96-byte seal; total size is a multiple of 4096.
A group can contain several canonical operations.

| Codec | Group contents before final padding/seal |
| --- | --- |
| 0 raw / 1 per-operation LZ4 | Repeated 192-byte entry header, encoded body, 8-byte alignment |
| 2 shared LZ4 | 64-byte shared header, entry headers, one compressed body block, 8-byte alignment |

Headers retain operation identity, chain/body digests, lengths, and codec and
remain uncompressed. Unknown codecs/reserved fields fail validation. Raw header,
descriptor, and padding space is reserved before collecting operations.
Exact layout: [segment codec](../ozzy-journal-segment/src/codec.rs).

### Compression

Actors prepare/compress bytes with reusable scratch while previous writes run.
Raw is default; LZ4 needs the segment `lz4` or facade/runtime `lz4-storage`
feature and `BodyEncoding::Lz4`. Compression is skipped without sufficient
saving or when it cannot reduce aligned physical size. Groups of prepared
producer blocks remain raw to avoid recompression.

APPEND, segment, and OMQ transport compression are independent. All preserve
record boundaries, retry identity, and confirmation policy. Whole producer
blocks can pass through reads; partial reads decode the selected extent.
Record/operation/segment limits must all permit an oversized record.

## Append, synchronization, and roll

1. Validate authority/retry/limits; assign offsets and freeze canonical bytes.
2. Prepare physical groups on the actor and submit owned bounded backend jobs.
3. Resume interrupted/short writes until complete; vectored calls honor slice limits.
4. Install ordered generation-matching completions and indexes; ambiguous failure
   fences the writer.
5. Publish required durable evidence, satisfy policy, then apply/reply.

```mermaid
sequenceDiagram
    participant J as Partition journal owner
    participant B as Storage backend
    participant A as AIO device owner
    participant H as Fixed file helpers
    J->>B: Owned append job, placement and generation
    B->>A: Bounded aligned data write
    A-->>B: Physical completion
    B-->>J: Matching completion ticket
    J->>J: Install ordered written prefix
    opt Durable evidence required
        J->>B: Publish captured durable prefix
        B->>H: Required file/metadata barriers
        H-->>B: Barrier completion
        B-->>J: Exact durable-prefix evidence
    end
    J->>J: Advance only the selected policy's eligible prefix
    Note over J,H: Cancellation retains buffers and handles until physical completion
```

| Write mode | Boundary |
| --- | --- |
| `O_DSYNC` default | Complete writes supply data-integrity barriers; disk groups still publish durable evidence |
| Buffered (RP) | Written progress only; flush, roll, promises, shutdown retain required barriers |
| Linux direct I/O | Aligned replicated data writes through `O_DIRECT`; headers/metadata/reads remain buffered |

Preallocation reserves full capacity/final EOF; appends advance a logical prefix.
`O_DSYNC` is not multi-write atomicity. AIO submission/completion alone is no
barrier. Metadata and directories require their own synchronization.

Write handles remain open through roll/shutdown. A four-entry read-handle LRU
caches exact path/source identities; jobs retain independent leases and file pins.
Cancellation releases observation, not physical buffers/handles/admission.

### Segment roll

Prepare/synchronize a new successor, then publish its manifest and `CURRENT`.
Buffered roll synchronizes the predecessor, successor header with range-limited
`RWF_DSYNC`, directory, manifest, and selection. Successor appends may overlap;
another roll, sync, or shutdown must settle publication first.

Selected files are immutable. Normal rolls use new IDs; replacement uses a fresh
`<id>.<incarnation>.log`. Manifests name exact incarnations; successor headers
bind predecessor ID/digest. Reads retain their original generation.

Optional `zero_ahead` prezeros disk-quorum successors in synchronized 4 MiB jobs.
It is off by default, doubles device writes, and can leave unselected successors.

### Operator tuning

| Bound | Tradeoff |
| --- | --- |
| Encoding chunk | Compression CPU/ratio versus preparation/cold-read latency |
| Ready physical batch | Throughput versus writer occupancy; no fill delay |
| Physical/decoded segment size | Roll frequency versus sync stalls, RAM, recovery work |
| RP backlog | Burst/stall tolerance versus RAM; no remedy for sustained disk deficit |
| Device depth | Aggregate ordinary jobs plus reserved progress capacity |

Roll frequency follows physical/encoded or decoded/canonical rate, whichever
limit arrives first. Backlog must cover write/roll stalls and already accepted
work. Transport aliases, reads, descriptors, and dirty pages require additional
RAM. [Calibration commands](../ozzy-bench/README.md#disk-calibration).

## Files and exclusive ownership

`OZZY_VOLUME` checksummed markers bind cluster, broker, and volume IDs in existing
device roots. Startup checks local identity before journal open. Missing/mismatched
volumes fail closed; initialization never creates roots or replaces markers.
A directory lock cannot fence copies elsewhere; persisted placement selects the
writable store.

Metadata is written to a new temporary file, synchronized, then published.
Interrupted temporaries select nothing; retry removes/recreates them. Existing
final names must match exactly. Offline relocation stops the owner, verifies/syncs
the copy, publishes placement, then retires the source. Online movement is absent.

## Integrity and formats

Domain-separated XXH3-128 checks physical/canonical integrity. The seed is
XXH3-64 of the domain's UTF-8 bytes; 32-byte slots contain the big-endian 16-byte
result plus 16 zeros. The zeros add no strength. Length/decoded bounds precede
allocation; framing, body digest, and chain must all validate. Checksums provide
neither authentication, authority, nor rollback protection.

### Durable recovery evidence

Disk groups publish the synchronized **accepted** prefix before voting, including
operations without a commit announcement. `DURABLE` stores two 192-byte records
64 KiB apart, each within one 512-byte sector. Publications overwrite both copies
and synchronize data without rename; sector writes are assumed atomic.

Recovery selects the newest intact in-scope publication and repairs the other
copy before writes. Two damaged copies fail startup. Evidence binds store,
group, broker, volume, configuration/metadata generation, segment, and accepted
position. A changed selected manifest fences asynchronous publication. Required
publication survives observer cancellation. Recovery never falls back to an
older manifest or temporary. Authorized new-active-ID replacement supersedes
older evidence under protected-history rules.

RP publishes running `MEMORY_VOTING` state before voting and drained state only
after closed admission and settled writes. Missing/stale/corrupt/running evidence
requires nonvoting recovery.

## Recovery and suffix installation

| Condition | Action |
| --- | --- |
| Normal open | Verify identities, authority, selected files, seals, chain, and anchors |
| Zero suffix | Unused preallocated capacity |
| Active damage after durable prefix | Discard/zero damaged group and later suffix; restore allocation |
| Damage at/before durable prefix | Refuse without changing bytes |
| Sealed/installed damage | Refuse strict open; authorized sealed repair may be possible |
| Corrupt index | Rebuild from validated segment bytes |
| Corrupt authority / lost store / unclean RP | Fail closed or remain nonvoting for recovery |

Replication authorizes suffix replacement. Protected committed fragments survive
physical regrouping. New files are staged, validated, synchronized, and selected
atomically; old selection remains authoritative until publication. Shutdown aborts
unpublished replacement without lowering durable promises. Staging/reads remain
bounded.

### Repairing sealed segments

Quarantine retains valid identity/configuration/selection/durability evidence.
Fresh authority from both other brokers permits bounded inventory and missing
range fetches from one frozen donor. Validated local operations may be reused;
replacement preserves original canonical boundaries. New physical incarnations
are synchronized, selected, and privately replayed before fenced restart/election.
Healthy files and damaged originals remain until safe cleanup. Active damage or
unavailable ranges requires full recovery or refusal.

## Indexes, checkpoints, and retention

### Finding and reading records

| Cache | Default bound |
| --- | --- |
| Active/predecessor operation backing/selectors | 512 MiB, oldest eviction |
| Cold offset indexes | 64 entries / 8 MiB |
| Cold decoded operations, reader and retry separately | 16 entries / 4 MiB; one decoder-bounded larger operation replaces others |
| Recent read handles | Four exact sources |

Recent caches retain stored encoded backing, not a permanent decoded copy.
Cold reads capture exact segment identity/range and verify indexed operations.
Capture does no file work; asynchronous readers execute through backends.
Missing derived indexes can be rebuilt from protected sources. Compact selectors
use stride/base/count for equal-size raw single-part records; other records retain
position pairs. IDs and multipart descriptors remain in immutable bodies.
Whole allocations, captured reads, cache metadata, and scratch have separate bounds.

Time seeks use APPEND-head timestamps; ID seeks search retained segments and
require explicit duplicate policy. An older durable source can be read while
later writes remain unsynchronized, without exposing those later operations or
advancing durable evidence. Captured reads/donor snapshots delay file deletion;
subscription cursors alone do not.

### Checkpoints and deletion

Streaming builds pin a published committed source and accept bounded fragments
through backend jobs, keeping one chunk open. Journal intake can continue between
fragments. Failed or canceled builders cannot select themselves. Installation
checks the captured position and source lineage before selecting the artifact.
Sequential reads retain one handle and hash bounded fragments. Decoded recovery
state stays private until every chunk and the complete state digest match.

Canonical checkpoint images start with a 104-byte `OZYCIMG\0` header: version
`u16` (1), six zero bytes, core snapshot length `u64`, partition incarnation
(16 bytes), and an identity header (64 bytes). The last 80 bytes are zero when
there is no identity section. Bounded core metadata follows, then identity rows;
decoding retains no second identity byte image. Bare snapshots are rejected.

Dedup index images stream separately from canonical metadata. Their 64-byte
header contains `OZYDIDX\0`, version `u16`, six zero bytes, then six `u64` values:
canonical operation, logical time, window, first live offset, exclusive record
end, and row count. Each 32-byte row contains message ID, offset, and logical
acceptance time. Integers are big-endian; enclosing checkpoint chunks supply
integrity and partition authority.

Capture includes only live confirmed identities. Encoding freezes expiry, with
bounded new-identity capacity, without copying the index. Restore checks policy,
offset order, unique IDs, and the time window before exposing the complete image.
An invalid or incomplete image supplies no admission state.

Partition creation fixes the optional dedup window in log milliseconds; zero in
the encoded `u64` disables deduplication. Deployment identity and canonical
snapshot format 6 retain that window and the coordinator group images. A changed window requires a new incarnation;
index capacity changes do not change it. Earlier snapshot formats are rejected.

The identity index belongs to the journal owner, outside cloned core images.
Validation reserves nothing; canonical acceptance installs stored identities and
confirmation advances their boundary. Duplicate attempts never refresh IDs.
Recovery must restore the matching identity image before admission; canonical
state alone cannot authorize an empty index or retirement of its source history.
All three modes preserve live IDs after payload retirement. Cluster checkpoint
transfer adopts the identity section before ordinary new-view activation.
Verified checkpoint ranges hash the whole physical chunk with bounded I/O scratch
and retain only the requested bytes, including when those bytes are near a chunk edge.
Sequential cluster transfer retains one chunk handle and reads each byte once.
Prefix fragments stay private until the receiver verifies the complete image.
Broker checkpoint capacity includes every configured dedup entry. Physical chunk
size grows to fit bounded directory metadata; I/O fragments remain at most 64 KiB.

Runtime checkpoint retirement starts in a settled turn, with accepted,
committed, and applied prefixes equal. That turn publishes the committed
position, captures the confirmed state, copies every live confirmed identity
row, and captures the sealed prefix to retire. A job then encodes, writes, and
reads back the checkpoint, revalidates a reused selection, and checks the prefix
against the committed floors while the partition keeps admitting. A segment
this owner rolled is checked from the record ends its read index held at roll,
bound to the segment's exact digest and operation range. Only segments sealed
before the owner opened are read and validated. Between writes, one owner
step revalidates authority files and publishes the checkpoint selection and
prefix retirement in one manifest. A view change abandons unfinished work; its
files become unselected leftovers. Selecting a position the current manifest
already publishes as committed needs no lineage rescan: installation validated
it, and no later change truncates a committed prefix. After a crash, a retry
whose confirmation was lost while the job ran resolves to its original offset
from the checkpoint plus the replayed suffix.

Configured retention changes become canonical `PartitionPolicy` operations.
Confirmation waits for application. Checkpoints contain state, producer
coordinates, retry floors, and original committed op/digest, never payloads.
Segments preserve required replay and exact retry results.

Retention confirms retry floors, then record trim; synchronizes/selects a
checkpoint before unreferencing the oldest sealed prefix. Repeated bounded passes
may reuse only the exact settled checkpoint position/digest, revalidating authority,
files, and protected floors. Surviving predecessor digests are never renumbered.
Expired readers receive explicit gaps.

Planning uses each sealed segment's record end and newest append time. A roll
takes them from the segment's read index; only segments sealed before the owner
opened are scanned, one per turn. Both count only batches that stored records,
as the active segment's read index does.

Eligible retention debt continues in bounded turns with a foreground turn between
steps. The periodic interval applies after catching up, not between retirements.
Maintenance waits for partition creation before deciding whether retention is disabled.

Retirement never changes application state. Its installation narrows the
committed and any speculative identity coverage to the retained history without
reading files: retired sources, their deletion protection, and overlay claims
before the first retained segment leave.
When every captured source retired, coverage re-anchors at the predecessor that
the first retained segment binds; lineage checks accept that anchor. New control
identities wait in a bounded overlay. Validation reserves
overlay capacity only for operation kinds that claim an identity; record
batches, partition creation, durable progress, and dedup watermarks claim none.
Progress retries resolve by their cumulative cursor and assignment fence. The settled
refresh stops admission while it indexes the active segment, so background
refresh waits until the overlay is half full. A full overlay drains intake until that refresh
runs. Each owner validates sealed index sources once; reuse checks the index
checksum/source binding. Built snapshots of the active segment resume after the
last validated group and sort and merge only appended entries. Reopen, explicit
repair and scrub validate physical bytes.
Index format 3 includes a dense 56-byte selector for every canonical operation,
including payload-free duplicate results. Numeric lookup selects one source and
revalidates the operation's physical framing and logical digest before use.

Age uses maximum segment append time; backward clocks cannot prematurely expire
newer records. An aged active segment rolls first. Byte targets include active
and sealed capacities per partition/broker. Settled turns also reclaim up to
eight unselected objects/class, even below retention limits, after each manifest
publication, while removable objects remain, or once a pinned one is released.
Cleanup changes no record/retry floors and preserves selection/pins.

Recovery transfers checkpoint state, required retained operations, and full
accepted suffix. Schema/digest/anchor/chain and destination publication all
validate before voting. Quarantine builds fresh private state; old files remain
for inspection until normal bounded cleanup.

Checkpoint reception writes bounded fragments into a private staging file.
Metadata and identity limits apply before allocation; canceled writes fence the
attempt. Adoption preserves the original image and rechecks its digest while
streaming into destination checkpoint chunks. Unselected input authorizes neither
voting nor admission; reopen cleans abandoned staging after physical jobs finish.

### Bounded maintenance

Streaming validation reads one physical group at a time and releases its bodies.
Scratch follows group/operation/read bounds, not segment capacity. Shared shard
admission reserves scratch before file jobs; pressure defers maintenance.
Canceled backend jobs retain physical ownership until completion.

| Work | Scheduling |
| --- | --- |
| Storage validation | `with_storage_validation`; 256 KiB / 2 ms cooperative steps |
| Metadata cleanup | `with_metadata_cleanup`; 32 objects/turn |
| Orphan cleanup | `with_orphan_cleanup`; bounded scan and selection/pin recheck |

Maintenance shares the device queue and alternates with settled foreground work.
Finite captured source lists fence new history; authority changes invalidate old
work, corruption fences it. Metadata cleanup deletes no payloads; orphan cleanup
advances no retention floor and excludes active staging.

## Supported failure model

Linux/Android local filesystems and process crashes are supported. Barriers rely
on filesystem/device honesty and stated atomic-sector assumptions. SIGKILL retains
OS page cache; it does not model power loss. Surviving group authority can permit
selected repairs. Valid rollback of authority metadata or an entire store is
outside the supported model.
