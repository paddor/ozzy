# Changelog

## [Unreleased]

### Added

- Optional per-topic first-wins deduplication with bounded identity windows.
- Durable consumer-group progress with individual acknowledgments and cumulative
  broker confirmations.
- `GroupReader` for automatic consumer groups: membership, sticky partition
  assignment, cooperative handoff, and resume from persisted cursors.
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

### Changed

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
