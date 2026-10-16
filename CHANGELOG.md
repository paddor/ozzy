# Changelog

## [Unreleased]

### Breaking

- `BrokerLinks::routes` is async. The SDK owner thread registers the topic,
  so routing state needs no lock shared with application threads.
- The re-exported `Context` and `Endpoint` come from omq-tokio 0.29 instead
  of 0.25.
- Segment group seals record a durable mark: the highest group already durable
  when the group was prepared. Segments written by earlier builds no longer
  validate; format stores again.
- `TransportLimits` has a `publication_messages` field that bounds PUB and
  SUB queues; `send_messages` and `receive_messages` bound PEER only.
- `TransportLimits` has a `peers` field: the most connections one PEER socket
  admits. Brokers set twice their clients, brokers, and repair aliases.
- Telemetry event `replica_publication_send_drop` is now
  `replica_publication_send_wait`: a full follower PUB socket keeps the frame
  and sends it once it has space.
- `TopicRecord::payload` is a `RecordPayload` instead of a `SmallVec`. It
  holds one part directly and more parts in one boxed slice, 32 bytes instead
  of 72, and derefs to `[Bytes]`.

### Added

- Optional per-topic first-wins deduplication with bounded identity windows.
- Durable consumer-group progress with individual acknowledgments and cumulative
  broker confirmations.
- `GroupReader` for automatic consumer groups: membership, sticky partition
  assignment, cooperative handoff, and resume from persisted cursors.
- `TopicReader::try_next` returns a record that is ready without waiting, so
  an application drains buffered records without an await per record.
- Optional shard and disk-worker telemetry with thread-owned OMQ PUB sockets,
  a shared IPC SUB collector, and bounded diagnostic logs.
- `Broker::ready` resolves once every local partition has recovered and either
  bootstrapped as leader or observed a remote leader.
- Telemetry counts leader proposals deferred behind unconfirmed originals or
  capacity, and journal work held for its owner, payload memory, or settled writes.
- Telemetry counts dispatcher commands, sends, refused destinations, and paused
  or dropped input, and SDK writers retrying unanswered APPENDs.
- Telemetry counts follower receive refusals by cause: full receive ledger,
  full pipeline window, or full queued arena.
- Telemetry logs the error that fences a replica journal worker, so a worker
  that reports only "faulted" still names its cause.
- `limits.max_clients` sets how many SDK link owners a broker admits, 128 by
  default instead of a fixed 32. Telemetry counts and logs each HELLO dropped
  at the limit.
- `BrokerLinks::inspect_group` and `BrokerLinks::delete_group`. Deletion fences
  the group's members on every partition and frees its name; readers of a
  deleted group stop with `GroupReaderError::Deleted`.
- Brokers and `BrokerLinks` accept `lz4+tcp://` endpoints with the
  `lz4-transport` feature. A deployment uses it for every endpoint or none.
- Brokers and `BrokerLinks` accept `zstd+tcp://` endpoints with the
  `zstd-transport` feature, under the same all-or-none rule.
- Telemetry counts retention progress: deferred turns, turns waiting for an
  earlier retirement, proposed floors, started and installed retirements, and
  removed unselected storage objects.
- Telemetry times the storage cleanup pass of a retention turn
  (`retention_cleanup`).
- Optional broker `admin` endpoint (`ipc://` or `inproc://`) for an admin
  console. It answers broker facts, event counters per thread, broker links
  and connected clients, and each local partition's replication status and
  committed state: record offsets, consumer groups, and their cursors. It has
  no authentication yet.
- `ozzy_telemetry::threads()` returns each live thread's event values by
  thread name.
- `ozzy-tui` (crate `ozzy-ui`), a terminal console of one broker's admin
  endpoint: broker, cluster, and link facts; partitions with record offsets
  and commit rates; replication progress and follower lag; consumer groups
  with members and cursor lag; event counters per thread with rates. Keys
  and mouse clicks pick screens, panes, and rows.
- Telemetry counts reader publications that waited for space in the
  broker's full PUB socket (`reader_publication_send_wait`), as it does for
  followers.
- `TopicReaderStats::live_gaps` counts live publications that arrived past a
  gap and moved the reader to replay.

### Changed

- A new segment, from a roll or from the log a new view installs, takes the
  partition's spare (`segments/spare.log`) when there is one. Cleanup keeps
  one unselected segment file as the spare instead of unlinking it. Appends
  then overwrite written blocks instead of converting unwritten extents,
  which costs a metadata commit per `O_DSYNC` write on XFS. A byte-limited
  partition can use one segment more than `max_bytes` on disk.
- `format` writes zeros once where a store's first appends land: segment 1
  of a local store, or a spare in a replicated store, whose first election
  installs its log into a new segment. That is up to 128 MiB per partition.
  Fixed-load p50 for 128 B records: disk quorum 4.6 to 3.3-3.5 ms at 100
  records/s, 9-10 to 5.1-5.5 ms at 1k/s, and 12.6 to 10.5 ms at 10k/s;
  single durable 2.9 to 2.1 ms, 2.1 to 1.2 ms, and 5.4 to 2.9 ms.
- Recovery ends an active segment's log at its first invalid group and
  accepts any bytes after it, such as groups from an earlier use of the
  file. It overwrites the seals of later groups of this use instead of
  zeroing the rest of the file. Sealed segments are read only up to their
  sealed length, and opening reads the whole active segment file, up to
  `segment_bytes`.
- Partitions publish `DURABLE` after 20 ms with no write in flight or after
  half a window of writes, instead of after every write, where it competed with
  writes for the disk. Fixed-load p50 for 128 B records: disk quorum 16 to
  9-10 ms at 1k records/s and 15.7 to 12.6 ms at 10k/s; single durable 2.6 to
  2.1 ms and 6.6 to 5.4 ms.
- Disk-quorum votes and single-durable confirmations leave when the `O_DSYNC`
  write completes instead of after the separate `DURABLE` publication, which
  now follows off that path. Fixed-load p50 for 128 B records: single durable
  3.7 to 2.7 ms at 100 records/s and 8.0 to 6.6 ms at 10k/s; disk quorum 5.6
  to 4.7 ms and 17.6 to 15.7 ms.
- Recovery refuses a damaged active-segment group that a later valid group
  marks durable, since that write had completed. A damaged group no later
  group marks still ends the log, as an unfinished write.
- Broker threads have slash names: `ozzy/dispatch`, `ozzy/shard/N`,
  `ozzy/disk/N`, `ozzy/disk/pN`, `ozzy/disk/aio`, `ozzy/admin`, `ozzy/drain`,
  and OMQ's `ozzy/omq/IO/N`, matching the SDK's `ozzy/sdk` and `ozzy/omq`.
- Retention removes unselected segments, indexes, checkpoints, and manifests in
  a job beside later writes. The partition stops only to capture its selection,
  instead of holding its journal for each pass, about 80-110 ms on average and
  up to 0.5 s at p99 on disk, which also delayed follower progress. A pass after
  a metadata-only publication removes old manifests alone.
- Deduplication keeps accepted-only and per-batch identity rows in a small
  table and moves them into the full reservation only while they outgrow it.
  Both tables had the full worst-case size, so every probe of a few hundred
  rows missed cache. Index work per unique record fell by about half, and
  inproc 160k attempts/s acknowledgment p50 from 23.6 to 22.3 ms.
- A partition confirms consumer-group progress updates that arrive while one
  is in flight together in its next proposal, up to 16 group cursors. With 20
  groups at 800 records/s, producer acknowledgment p50 fell from about 99 to
  57 ms on disk quorum, and each update still gets its own reply.
- Retirement encodes, writes, and reads back its checkpoint and scans the
  retired prefix in a job beside later writes. The partition stops only to
  capture state and identity rows in a settled turn and for one manifest
  publication between writes, instead of about a second per retired segment
  under load.
- Retirement checks a segment the broker rolled against the record floors from
  the record ends its read index held at roll, instead of reading and hashing
  the whole segment. Only segments sealed before the broker started are still
  scanned.
- Retention planning takes a sealed segment's record end and newest append time
  from its read index at roll, instead of scanning the whole segment while the
  partition admits nothing. Age retention of sealed segments ignores batches
  that stored no records, as it already did for the active segment.
- Retirement narrows control-identity coverage to the retained history instead
  of rebuilding it. The rebuild indexed every surviving sealed segment while
  the partition admitted nothing, for seconds per retirement under load.
- Retention turns repeat storage cleanup only after a manifest publication,
  while unselected objects remain, or once a pinned one is released, instead
  of on every settled turn. Each pass lists directories behind data writes
  while the partition admits nothing.
- Durable consumer progress no longer claims a control identity; a retry
  resolves by its cumulative cursor and assignment fence. Progress validation
  no longer waits for the partition's earlier writes to settle, and progress
  no longer fills the identity overlay, so many consumer groups slow
  producers far less.
- Writers accept an APPENDED range that spans consecutive in-flight requests
  when its request ID names one of them, as the protocol allows.
- Other producers keep appending while a redundant producer's duplicate waits
  for its unconfirmed original. Only that producer's later appends queue behind
  it, instead of every append to the partition.
- Deduplicating partitions commit their whole identity-index reservation when
  the index is created, so admission takes no first-touch page faults.
- Replicas decode APPEND bodies once during admission instead of also decoding
  them to look for control identities they cannot carry.
- Telemetry reads the clock once per 64 recorded operations instead of on every
  counter update, so quiet threads publish less often than every 20 ms.
- `ShardContext::shutdown` and `FrontendContext::shutdown` are
  `ozzy_io::signal::CloseSignal` (`close`, `closed`, `is_closed`). The broker's
  own `Shutdown` type is gone.
- Input that reaches a partition at the end of a shard turn wakes the shard
  instead of waiting for its 2 ms timer sweep, and proposals queue on the
  partition's thread instead of through a cross-thread channel. Inproc
  acknowledgment p50 fell from 21.4 to 20.7 ms at 160k attempts/s and from 14.1
  to 13.4 ms at 2000 attempts/s.
- Broker shards sleep until the earliest partition timer instead of waking every
  2 ms. Each turn visits only the partitions that were woken or whose timer is
  due, plus a sweep of every partition each 100 ms as a safety net. Telemetry
  counts `partition_sweep_progress` when only the sweep found work.
- Telemetry events are thread-owned atomics. `ozzy_telemetry::totals()`
  combines every thread's values, including exited threads, by each event's
  `Kind`: count, peak, or gauge. Events always count on every thread; stage
  timing and PUB snapshots stay opt-in. Journal read, write, delivery,
  progress, and dedup counters are telemetry events; the `storage-metrics`
  feature and its process-wide counters are gone.
- A partition takes no new appends while its framed reader publication waits
  for the broker's publication slot, so intake cannot run ahead of live
  delivery on a busy core. Single-broker saturation delivery p99 for 128 B to
  8 KiB records fell from 68-99 ms to 48-63 ms.
- SDK readers keep raw payloads as slices of the received frame and give a
  decompressed delivery one shared owner, instead of allocating an owner per
  record. At disk-quorum saturation, reader allocations fell from about 211k
  to 5k per second.
- Broker PEER sockets queue up to 1024 messages per connection instead of 64,
  capped by OMQ at 16 MiB or one maximum message, whichever is larger. Reader
  and follower PUB/SUB queues stay at 64 messages, since SUB has no byte limit.
- Leaders send fewer replication packets per append. Followers report PUB
  receipt unasked, so new work no longer triggers an immediate probe, and a
  COMMIT or vote that repeats the last one sent waits for the heartbeat or
  retransmit timer. At disk quorum and 1k records/s, replication packets per
  append fell from 15.6 to 8.7, broker allocations by 22%, and broker CPU by
  about 7%.
- Broker shards take fewer turns per append. A turn takes queued input before
  it visits partitions, a partition visit repeats up to four rounds while it
  progresses or wakes itself, and freed reply slots or returned port capacity
  wake the shard only after a refused send. At disk quorum and 1k records/s,
  shard turns per append fell from about 36 to 20 and broker CPU from
  0.81-0.90 to 0.74-0.78 cores.
- A full reader or follower PUB socket on the broker keeps the frame it
  refused and sends it once OMQ reports space, instead of dropping it for
  PEER repair. That socket takes no other frame meanwhile, so the shards
  behind it hold intake.
- SDK PEER sockets accept up to 16 MiB per connection or one maximum message
  plus the reader reservation, whichever is larger. OMQ charges received
  frames to their connection until the reader drops them, so a large record
  no longer stalls the link.
- `TopicReader` spends less CPU per record. A buffered delivery reads no
  clock, raw records skip the decoder, the partition round-robin needs no
  division, and the per-record accessors and disabled stage diagnostics
  inline across crates.

### Fixed

- Let a deduplicating follower wait to validate replacement identities until it
  applies the commit that freed their capacity on the leader. A follower whose
  disk barriers or commit announcements lagged failed its partition with
  "deduplication identity capacity exhausted".
- Run record retention in every settled retention turn of a deduplicating
  topic. Under steady load identity expiry always had more work, took each
  settled turn, and never let retention retire history.
- Serve every reader subscription of a partition before its owner sleeps.
  Each poll visited 16 slots, empty ones included, and woke again only after
  progress, so with mostly empty slots a lone replay waited for unrelated
  traffic and could stall.
- Stop a writer's driver as soon as the writer fails or closes. A dropped
  writer whose input was gone kept finding its own failure as new work and
  starved every other writer on the same SDK thread, so their `close` hung.
- Reply to deduplication capacity errors with NACK 10 (retry after backoff)
  and to invalid deduplication batches with NACK 1, instead of an unknown
  outcome.
- Size a follower's pending canonical transitions and pending dedup identities
  at the two live windows it retains. A follower whose disk barriers lagged
  could accept more than it could hold unapplied and stop its partition.
- Reserve control-identity capacity only for operations that claim one. Record
  batches on deduplicating topics counted against it, so a follower with a
  small identity overlay could stop validating its staged suffix.
- Apply a follower's accepted operations when the leader announces a commit
  past them. A follower that fell behind the leader's commit with a full live
  window waited for an announcement it could accept, never released the
  window, and pinned the leader's history until the storage budget ran out.
- Let a follower retain two live windows of accepted operations beyond its
  released prefix, admitting what the leader keeps in flight beyond the commit
  it announced. A follower still persisting one window when all three brokers
  stalled together refused the leader's next window and repaired it.
- Run identity expiry in bounded steps between foreground turns instead of
  draining the partition's pipeline for a settled retention turn each time more
  identities expire. Under load the repeated drains burst the leader's live
  window and made followers refuse and repair packets.
- Size a follower's receive ledger at two live windows, so a leader that keeps
  its window full is no longer refused and repaired once the follower's
  released prefix trails the leader's applied prefix.
- Send a shard's replies to one destination in the order it produced them. The
  shard's outbound queue could hand a later reply to the dispatcher first, and a
  reply refused for capacity could be overtaken, so the SDK saw confirmations
  out of order and backed off to replay its window.
- Confirm repeated and older deduplicated IDs from the same producer with their
  original offsets and an explicit stored/duplicate receipt disposition.
- Keep deduplicated ingestion progressing through identity expiry and temporary
  payload-memory pressure, without refusing small appends based on maximum batch
  capacity.
- Bound telemetry log publication rates and background I/O threads.
- Keep consumer progress advancing after producers stop by releasing older
  cached replication packets that retain bounded control arenas.
- Retry consumer-group activation without waiting for the next heartbeat.
- Release revoked consumer-group sources within the local drain bound even when
  the application stops polling the reader.
- Rejoin fenced consumer-group readers automatically from confirmed progress,
  fencing tokens from the previous membership instance.
- Wait for prior ownership evidence or the drain deadline after a consumer-group
  coordinator failover.
- Keep replicas and recovery deterministic when consumer progress covers
  records accepted in the same unconfirmed suffix.
- Stop rewriting the whole deduplication preparation table for every proposal.
- Defer background control-identity refresh until its overlay is half full. Each
  refresh stopped its partition for time proportional to the active segment.
- Shorten partition stalls from identity refresh and retention snapshots: built
  snapshots index only active-segment groups appended since the previous one,
  and index sorts yield per turn of work instead of every 64 steps.
- Keep brokers progressing under overload: the dispatcher sends a queued reply
  before admitting more shard work, and waits for send progress instead of
  retrying full destinations on every turn.
- Stop broker dispatchers from spinning on a congested peer when OMQ reports
  send progress that a retry still cannot use; such retries back off briefly.
- Start no retention or identity-expiry turn while an earlier turn's proposal
  still waits to validate. Under sustained deduplicated load the second
  proposal found the slot taken and stopped the broker shard with a panic.
- Free a retired segment's space as soon as cleanup removes it. Cached read
  handles kept up to four removed segments per partition allocated, beyond
  the byte retention limit.
- Let a restarted retained-topic copy with no stored operations ask its peers
  whether its history expired, as other restarted copies do. It used to start
  an election it could not win, which stalled the other two copies' writes
  for an election timeout before it recovered. The first election of a newly
  formatted retained partition now waits for that probe, up to 500 ms.
- Release a closed store's group lock even while a child process that the
  application is starting still holds inherited descriptors. Reopening the store
  in the same process failed as locked until the child ran exec.
- Follower shards hand the leader's `PREPARE_PUB` copies to the data path.
  The control path dropped them for lacking a link session, so every append
  reached followers only through a probe, a state reply, and a catch-up
  `PREPARE_FLOW`.

## [0.1.1] - 2026-10-09

### Breaking

- Rename the broker executable from `ozy_broker` to `ozzy`.

### Fixed

- Keep retention running after broker restart and repeated segment retirement.
- Recover replicas safely when retention has removed different history prefixes.
- Prevent incomplete or stale donor state from being used during recovery.
- Report the first broker failure through shutdown instead of losing its cause.

## 0.1.0 - 2026-10-07

Initial release of the Ozzy broker and native Rust producer and consumer SDKs.

- Single-broker durable, disk-quorum, and replicated-persisting modes.
- Producer identity resume and explicit takeover, with broker-owned retry identity.
- Live consumption, checkpoint replay, and bounded retained history.
- OMQ transport and a shared inproc simulator with workload churn and faults.
