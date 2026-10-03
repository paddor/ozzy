# Replication and recovery

The current replication core follows Viewstamped Replication (VSR) with exactly
three fixed brokers.
The code calls them voters/replicas and calls the leader primary. One leader
orders operations for one partition; two followers retain copies.
This is a crash-failure protocol, not Byzantine consensus or arbitrary disk
corruption repair. [Protocol](PROTOCOL.md) owns wire schemas;
[storage](STORAGE.md) owns durable publication.

The implemented core is fixed at three members. The selected development
deployment adds an explicit one-broker configuration using the same native
protocol and segment path. Its confirmation is local durability, not a
three-broker quorum. Bootstrap/runtime integration remains separate from the
single-broker state machine described below.

## Configuration and authority

Replicated placement: exactly one group per partition. Every partition uses the
same three broker processes, all storing every group. Lag does not change
placement. Topic metadata binds each partition to its own group identity,
configuration, and policy. A group cannot span partitions. Partition actors on
one shard can have different leaders and views, including within one topic.

Choose immutable broker order at partition creation to distribute initial leaders:
`[A,B,C]`, `[B,C,A]`, or `[C,A,B]`. The existing selection rule is
`brokers[view % 3]`; all members persist the same ordered configuration.
Each partition elects independently. Neither a routing hint nor reordering a stored
configuration may bypass election. Ongoing load balancing is not implemented.

Group ID, configuration epoch/digest, ordered broker identities, policy, and
view scope every vote and transfer. Membership order determines leader rotation.
Endpoints, timeouts, and thread counts are deployment settings, not votes.
Routing IDs and configured principal fingerprints do not authenticate peers;
the adapter must supply the trusted/authenticated binding.

Disk `ConfigurationRecord` is a fixed 256-byte, network-order record binding
exactly three distinct identities/principal fingerprints, confirmation policy, protocol
version 1, and canonical schema 1. Its version-2 checksum uses the integrity
profile in [Storage](STORAGE.md#integrity-and-formats). Exact bytes
and reserved fields are enforced by its codec and fixed-byte tests.

Format writes/synchronizes immutable `CONFIGURATION` under the group lock.
Open requires its exact bytes before journal recovery or voting. Missing,
changed, truncated, oversized, or non-regular files fail closed. Generic journal
open cannot grant replica authority. Relocation preserves the exact binding;
ordinary roll, checkpoint, and suffix replacement do not edit membership.

Each broker owns one mutable replication state machine and separate journal per
partition group. The selected actor owns both on one application shard. File
operations use async storage backends, without moving authority to I/O workers.
The dedicated journal-owner runtimes have been removed. Shard counts and
group placement are broker-local and may differ. Multiple groups share threads, never logs, views,
or confirmations. One partition's repair cannot truncate another's files.
Each actor runs one group; shards host multiple actors. View changes fence old actions;
async work also binds a fresh local writer/memory incarnation. Distinct
configuration, view, link session, receive epoch, and writer incarnation are
not interchangeable generations.

## Future six-broker groups

Keep a path to six copies of each partition, with two brokers in each of three
sites. This is one six-member group, not two independent three-member groups.
It is not implemented and does not change today's three-member voting rules.

[TigerBeetle's flexible quorums](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vsr.md)
use three replicas for replication and four for leader change at size six.
Those sets must overlap because `3 + 4 > 6`. Intersection is a necessary
condition, not a complete recovery or consensus proof. Do not merely replace
the current quorum constant. Durable promises, history selection, lost-store
recovery, and RAM-confirmation restart rules all need qualification.

With two brokers per site, losing one site leaves four. Confirmation must cross
a site boundary. Geographic distance therefore affects write latency.
[TigerBeetle recommends nearby sites](https://docs.tigerbeetle.com/operating/cluster/).

Design boundaries to preserve now:

- Membership, site placement, and quorum policy belong to authoritative group
  configuration. Keep confirmation, election, and recovery thresholds distinct.
- SDKs route by partition/group and leader. Future discovery uses a bounded
  member list, not public `follower_1` and `follower_2` fields.
- Fanout, catch-up, and retained buffers need per-member and aggregate bounds.
  Additional brokers must not create unbounded queues or per-partition sockets.
- Keep fixed-three validation in existing codecs until larger groups are
  implemented. Do not add arbitrary quorum settings or claim online membership
changes. Single-broker development is explicit, never an automatic fallback.

## Single-broker authority

`local::Driver` admits a bounded contiguous canonical suffix and tracks local
write, durability, and application separately. It uses the same validation fences
and journal completion tickets as replicated partitions. Only an observed data
barrier plus durable-prefix evidence advances confirmation. Applying that prefix
releases live capacity and permits replies. There are no votes or elections.

Its immutable 128-byte `local::Configuration` binds one broker, principal,
partition group, configuration epoch, protocol/schema, and integrity profile.
The codec checks all fields and its domain-specific checksum. Local and
fixed-three records reject each other's encoding. Exact store recovery supplies
the restart prefix and a fresh journal generation before local admission resumes.

## Normal operation and confirmation

Thread sequence: [RAM confirmation with background persistence](RUNTIME.md#replicated-confirmation-with-background-persistence).

1. Validate the request against committed plus pending state. Reserve count,
   byte, and result capacity; assign offsets and freeze canonical bytes.
2. Register the operation/digest before starting asynchronous work. Leader
   journal append and follower PREPARE transmission may overlap.
3. Followers validate scope, leader/view, predecessor/digest, schema, application
   transition, and limits before accepting the complete operation.
4. For disk quorum, a follower votes only after its exact contiguous prefix is
   synchronized. The leader requires its own durable completion and one distinct
   matching follower vote. For replicated-persisting, both must retain
   validated bytes; persistence continues in the background.
5. Commit/apply eligible operations in order, then complete writer requests from
   confirmed applied state. Announce the commit prefix through piggyback or idle
   COMMIT; followers apply only when their required local evidence exists.

Worker validation binds the exact broker, authority, writer incarnation, accepted
history, and applied image. A newer confirmation on that unchanged history does
not invalidate the plan: it changes neither application image until applied.
Changed accepted/applied state or authority still requires fresh validation.

TCP delivery, queued I/O, page-cache writes, and flow receipts are not disk
votes. Retained-RAM votes must never satisfy disk-quorum requests. A cumulative vote
cannot skip a predecessor or claim a different digest. The slow follower need
not block a healthy pair, but one remaining broker cannot confirm alone.

A leader may die after replying but before announcing COMMIT. The next leader
must preserve that quorum-held history even if no survivor marked it committed.
Local client-success flags are not the distributed proof. Don't add a mandatory
second sync for every reply merely to persist that flag; reconstruct results
from the selected canonical history. Exact retained retries return that result.

## Receipt, credit, and repair

The journal-backed core also supports votes from retained RAM while buffered
writes proceed. Its live window is reclaimed after application and completed
writes. Written and durable positions stay separate.

Before promising a new view, brokers flush all accepted history. A disk prefix
cannot prove that no later RAM vote existed, so unclean intact-disk restart is
refused. Drained restart requires verified shutdown or fresh recovery evidence
and still fences the old view.

`JournalConfig::write_pipeline` bounds accepted canonical bytes in RAM. The
partition actor compresses chunks while the device writer is busy, collecting at
most one segment within the backlog. Ready chunks enter buffered vectored
writes without waiting for a full segment.

Ordered completions install written progress, never stable-storage evidence.
Confirmation and application advance independently. Application and writing
permit receive credit to return; a full backlog backpressures writers. Shared
shards additionally reserve aggregate capacity before advertising another grant.

Interrupted writes retry; short writes resume at the unwritten offset. Other
write, writeback, or sync errors fence the journal, including when idle.
Failed batches claim no success
even if bytes reached disk. Restart evidence remains unproven; the embedding
application owns process restart. See [pipeline bounds](RUNTIME.md#replicated-confirmation-with-background-persistence).

Graceful shutdown closes admission, drains accepted work, then publishes exact
`MEMORY_VOTING` evidence. Unclean startup refuses normal voting and requires
`RecoveringJournal::quarantine` plus fresh recovery from both other brokers.
Recovery replaces the complete selected history; stale local history is not
selectively reused in this policy. Ordinary opening records running state before
returning any voting authority. Missing or damaged evidence never authorizes
voting from an older disk prefix.

Losing every volatile copy can lose the unpersisted tail. Automatic restart of
an entirely unclean cluster is not implemented: without healthy recovery peers,
the runtime stays fenced. This mode does not promise disk durability at writer
confirmation or recovery after losing more than one broker's state.

Flow receipt describes contiguous bytes retained by one receiver. It grants no
group vote or durable position and does not release required history.

The commit-progress deadline is separate. Written progress resets the local
storage deadline in replicated mode; durable progress resets it in disk quorum.
RAM confirmations and transport probes cannot hide a stalled disk. A stalled
leader stops heartbeats and requests a leader change; the healthy pair can proceed.

Each receiver owns a fresh nonzero receive epoch. Resetting staged state,
changing writer/view, or restarting fences old PEER payloads/reports before capacity
is reused. Socket reconnection alone does not reset durability or grant credit.
Let S be cumulative unique sends, A cumulative releases after application,
and C fixed capacity. Background-persisting groups also require completed writes
before releasing capacity. Automatic per-partition windows use G = A + C.
Shared-shard receivers start with G = 0 and require explicit reservations.
Their retained work plus unused grants cannot exceed C. Both operation and byte
checks must satisfy S + batch <= G. Retransmission retains its original charge;
receipt alone does not enlarge G. Counters/revisions never wrap.

A replacement epoch starts from an exact applied base with all retained
accepted suffix bytes already charged. No within-epoch retraction. Reopening
requires a correlated probe and verified local history; repeated/stale reports
cannot multiply credit or erase outstanding charges. Bound both sender metadata
and receiver storage independently.
An authenticated notification of another receive epoch advances the next probe
to the current turn. Duplicate hints do not advance it again. The hint installs
no credit and preserves any live probe and pending history check.
Shared-shard receivers discard unused credit on epoch replacement and await new
reservations. Retained accepted history stays charged. Protocol counters do not
replace [dispatch and buffer-lifetime accounting](RUNTIME.md).
Probes carry a separate available-operation hint so zero-credit followers can
request capacity for unsent work. The outstanding tail still bounds correlated
repair. Neither field proves confirmation; both stay frozen across probe retries.
New work after a completed exchange advertises availability immediately instead
of waiting for the unchanged-status probe delay.

Packet limits and total live capacity are separate. Sending a smaller packet
must not shrink the whole window. A lost packet uses bounded correlated repair;
send credits advance only when the bounded outgoing slot owns the frame. After
application, a slow third broker catches up from retained history/journal, not indefinitely pinned
writer arenas. Missing history or quota exhaustion is explicit, never silent
truncation of confirmed work.

`ActorConfig::replay_cache` independently bounds applied packets retained in RAM
by operation count and body bytes. It covers at least one live window; matching
flow metadata covers both windows. Eviction never waits for a slow follower.
Older history falls back to journal reads.

Cold catch-up runs on the existing shard reader. One bounded chunk can be pending
independently of normal appends. The reader reuses its verified segment/index;
background writes may extend beyond its captured completed prefix. The actor
checks leader scope, journal generation, and follower cursor before sending.
Shutdown joins the read; leader changes discard stale replies.
See the [catch-up data flow](RUNTIME.md#follower-catch-up).

If remaining byte credit cannot fit the next retained operation, the journal
returns its verified required size without advancing the replay cursor. The
actor waits for sufficient credit rather than failing the journal or repeatedly
reading the same bytes. That wait is bound to the broker channel, scope, writer
generation, and exact predecessor; replacement history cannot reuse it.

## Live fan-out

Journal-backed actors publish each live group once on a common group topic.
One SUB per configured remote broker preserves publisher identity. No recipient
masks or follower credit checks restrict fan-out.

A follower holds one shared publication when a gap or capacity limit prevents
admission, then pauses SUB reads. Further messages remain in bounded OMQ queues.
PEER repairs only through the held publication's predecessor. Controls and
journal completions continue. The held message consumes
no admission credit, so its missing predecessors can still arrive. Resume after
repair/release; ignore duplicates. HWM losses trigger another repair.

PEER carries confirmations, elections, status, and bounded repair. A PUB
receipt beyond PEER reservations needs independent leader-history validation
before it advances the repair cursor.

Live receipt pauses speculative repair. Known gaps repair immediately; a
missing final publication repairs after `flow_probe.maximum` without progress.
Duplicates do not extend the wait. Outstanding PEER chunks also retry only
after receipts stop advancing. `ozzy-core::live::LiveProgress` owns this rule.

New views, generations, and receive resets clear held state. Queued bodies
still face current authority, chain, and admission checks.

## Leader change

A lone timeout sends volatile EXIT_VIEW suspicion. Two current-view requests
authorize departure; a lone deaf broker cannot ratchet the view. A newer durable
promise fences old authority immediately. EXIT_VIEW is not a durable election
vote and cannot substitute for START_VIEW_CHANGE/DO_VIEW_CHANGE evidence.
A broker that already left a view answers a request to leave it with its own
EXIT_VIEW for that view. Otherwise a peer whose request completed the other
broker's quorum waits for that broker's durable promise, which may be stalled
behind its disk. The answer goes only to the requester, only while the
answering broker's own promise is pending, and at most once per retransmission
interval for each requester. A broker that also left the view cannot tell an
answer from a request, so an unlimited answer never stops.

Stop old-view normal acceptance and replies before collecting a new view.
For disk groups, settle admitted writes and persist the promise before sending
view-change votes. Reports include everything that may already have been voted
for; pending I/O is not permission to report a shorter history.

Collect distinct, scope-matching START_VIEW_CHANGE participants and complete
DO_VIEW_CHANGE reports. Select highest last-installed-normal-view, then longest
operation prefix, preserving every protected committed floor. Equal-ranked
conflicting digests halt selection. Never merge independently selected partition
tails or treat missing chunks as an empty suffix.

Fetch missing bytes from a matching immutable source. Verify the entire selected
lineage, install history/view metadata, and distribute START_VIEW. Disk brokers
durably install before voting. The leader reestablishes a new-view follower vote
through the selected tail, even when that tail was already known committed,
then applies and activates. Stale completion callbacks cannot restore authority.

Failure to finish advances the ordinary bounded election protocol; there is no
force-promote, lower-quorum, or discard-confirmed-history shortcut. Last normal
view describes installed history, not the largest view attached to any entry.
Forwarding an old operation into a new view preserves its original digest.

## Restart and recovery

Intact disk reopen validates configuration, canonical history, durable promises,
and installed normal-view lineage, then uses a fresh writer incarnation and a
fenced election. Never reuse an old leader `(view, op)` for different content.
A promised but uninstalled view does not grant normal authority. Bootstrap is
explicit creation of a new group, not a restart or missing-store fallback.

A lost/rolled-back/ambiguously corrupt store stays nonvoting. Recovery obtains
fresh nonce-scoped authority from the existing group, not an old local snapshot.
The recovering identity does not count toward that authority. Tickets bind
configuration, view, source incarnation/generation, full accepted tail, and
committed anchor. Capture all accepted operations: a delayed pre-crash vote
can still confirm work beyond the announced commit floor.

Full replacement transfers from genesis in bounded correlated FETCH_OPS/OPS
chunks. Verify request, donor, predecessor, count/bytes, canonical hashes/schema,
and application state before exposing anything. Freeze one exact snapshot per
attempt; don't combine
chunks from changing source generations. New authority supersedes the attempt,
settles its admitted I/O, and starts a fresh exchange.

Disk replacement is persistently fenced by a recovery marker in CONFIGURATION.
Ordinary configured open rejects it. Synchronize selected WAL/view metadata,
privately validate application history, then atomically publish/synchronize the
real configuration. Only a live matching recovery core may hand off through
fenced intact restart. Local marker bytes never authorize voting by themselves.

Physical sealed-file repair uses the same fresh two-broker authority and pinned
donor, but requests only missing canonical ranges. Intact local operations may
be reused after independent checksum validation; exact original manifest boundary
digests and full private semantic replay remain mandatory. Repair preserves local
accepted/committed history and last-normal view, and retains any higher durable
promise. It does not pretend to install the donor's full current history. Its
completion enters fenced intact restart; the normal leader-change protocol then
resolves any accepted-only suffix. See [storage repair](STORAGE.md#recovery-and-suffix-installation).

This requires two other normal brokers. Combining disjoint damage across several
nonvoting brokers needs additional distributed recovery logic. Checksums alone
cannot decide which incomplete history the group previously confirmed.

## Read authority and future extensions

Committed replay exposes applied records, not speculative intake. A stale
follower cannot answer writer retries as leader. Strong latest-state reads need
current authority/quorum confirmation, not just a locally cached leader label.
Lease/read-index-like optimizations require their own timing and safety proof.

Native subscriptions push bounded confirmed history from groups.
Group-aware readers follow leaders; explicit normal-follower reads remain possible.
Both check group/configuration, view, partition, owner, and link-session fences.
Reaching an applied end parks the cursor until more confirmed history arrives.
Committed retention floors return an explicit earliest-offset gap;
readers neither expose accepted-only records nor pin history against expiration.
Physical reclamation remains separate from these logical read floors.

A disk-group leader can also publish confirmed, applied records once for all
live readers of a partition. A publication is no vote, no confirmation, and no
proof of its sender's authority: readers accept only the source that their own
subscription confirmed, and repair every gap by replay. See
[RUNTIME.md](RUNTIME.md#live-reader-publication).

Checkpoint-based transfer, online membership, partition movement, arbitrary
corruption repair, and public replicated Node-mode integration are separate work.
Storage checkpoint primitives and reserved wire opcodes do not
enable these services. Movement must fence the source before destination
activation; a directory update alone cannot transfer ownership.

Use [Validation](VALIDATION.md) for production-core schedules, process faults,
and cross-host checks. Passing a finite simulation or happy-path test does not
prove complete consensus safety, power-loss behavior, or arbitrary host-loss
availability.
