# Ozzy protocol

Ozzy commands are ordinary OMQ multipart messages after the ZMTP handshake.
Routing identity is OMQ metadata, not a Ozzy field. SDK-batched APPENDs, control,
and confirmations use PEER. SDKs have no PUSH sockets. Live reading retains
broker PUB and SDK SUB sockets, with PEER for subscription, replay, and repair.
Payload bytes stay opaque.
Never extend ZMTP for Ozzy features. Transport receipt is not application confirmation.

The runtime carries native writer traffic over shared PEER broker links.
Each topic partition has its own writer state, leader, and retry identity;
partition count does not increase SDK connection count.

## Protocol

The native writer frontend implements HELLO/WELCOME, APPEND/APPENDED, and typed
NACKs for preprovisioned producer sessions on group broker endpoints.
`BrokerLinks` shares established broker sessions. `SharedTopicWriter` selects
partitions, pipelines individual records, follows partition authority, and
receives per-writer confirmation ranges.

Each range maps contiguous writer sequences to contiguous partition offsets.
Interleaved writers can require several ranges for one retried APPEND. Partial
replies retain the APPEND request slot until every record is confirmed. SDK
receipts keep exact offsets independently of later replies or batch boundaries.

Replica codecs live in `ozzy-replication::wire`. Embedded broker tests use
the same envelope, handshake, reader codecs, and packed record table. Wire
version 1 is the only version; there is no compatibility decoder. Readers
subscribe at an offset and receive confirmed records within explicit credit.
Local targeting never fabricates group authority.

Remote producer opening, directory commands, and dynamic broker provisioning
remain integration work. An allocated opcode does not imply implementation.

### Selected topic and session contract

Authoritative topic metadata binds one topic incarnation to its immutable
partition set, stable partitioner parameters, and limits. Each partition maps
to its own group identity, configuration, and policy. Replicated production
groups currently use the same three brokers and their shared endpoints.
Ordered membership may differ to
spread initial leaders. Reject duplicate group bindings across partitions and
inconsistent incarnations/configurations. SDKs cache authority per partition's
group, not one topic-wide or cluster-wide leader. Local shard IDs are not routes
in SDK metadata or identities in replication messages.

The selected single-broker development deployment uses the same SDK commands
with an explicit local-durable policy and one authoritative broker. It does not
advertise quorum replication or silently accept a weaker policy than requested.
Losing contact with members of a replicated group never changes its membership
or confirmation boundary. Future larger-group metadata must preserve a bounded
member list without making SDK sockets depend on the number of followers.

Topic creation defaults to 16 partitions; an explicit positive count overrides
it. SDK keyed selection is `XXH3-64(key_bytes, topic_seed) % partition_count`
into the authoritative partition order. Persist the algorithm, seed, count, and
order with topic metadata; all SDKs must agree. Counts need not be powers of
two. These routing parameters are immutable in the initial contract: adding
CPUs changes broker-local placement, not key routing. Online expansion requires
a separate ordering-preserving transition and is not implemented.

The application submits a record and optional key. SDKs choose the partition
before assigning its writer sequence or batching. APPEND contains exactly one
partition and one producer epoch; mixed keys are allowed when they map to that
partition. Keyless placement is sticky per SDK batch. The admitted destination
never changes on retry or leader refresh. Routing keys need not be stored unless
the application includes them in its record.

HELLO/WELCOME establishes a broker link, not an exclusive partition attachment.
Logical writer opening binds a partition, producer identity/epoch, policy, and
bounded credit within that link. Many writers may target one partition; one link
may carry multiple topics and partitions. Request correlation is unambiguous
across the whole link. Confirmation ranges remain scoped to group, partition,
writer, and current session; a reply for one writer cannot complete another.

Writer credit reserves APPEND slots and byte/record budgets at the destination
and the dispatcher-to-shard lane. Grants bind the current link/writer
generation; unused grants remain charged. Queue-slot credit and retained-payload
credit return at their respective release boundaries, not automatically together
at confirmation. The exact grant wire schema and remote writer opening remain
implementation work; local SDK windows alone do not establish broker capacity.

The frontend routes using clear metadata. The destination owner validates group
authority and partition membership before admission; metadata routing is not
authorization. Wrong-leader replies identify the affected group. No transparent
broker-to-broker forwarding of SDK APPENDs. Credit and dispatch must preserve
[destination isolation](RUNTIME.md#backpressure-isolation).

Wire authority: [native codecs](../ozzy-proto/src/envelope.rs) and fixed-byte tests;
[replication](REPLICATION.md) defines voting/recovery semantics. All integers
use network byte order. Reject unsupported versions/flags, malformed lengths,
overflow, and trailing fields before allocating or admitting work. Never infer
machine behavior from diagnostic text or silently weaken a requested policy.

### Envelope and primitive encoding

Full commands use three Ozzy frames after OMQ routing metadata:

```text
[64-byte envelope] [typed metadata bytes] [packed payload bytes]
```

Routine replica receipt/credit updates use one 45-byte `REPLICA_CREDIT` frame:
`opcode:u8, handle:u32, revision:u64, received_op:u64, received_bytes:u64,
operation_limit:u64, byte_limit:u64`. The full `REPLICA_STATE` binds the handle
to its peer session, group, configuration, view, receive epoch and base prefix.
Handles are nonzero, never reused within a process, and retired with the session.
Opening and repair responses keep full history hashes and correlation. Compact
reports never vote or confirm persistence. The leader restores a receipt hash
only from its own bounded outstanding-operation ledger; unknown history waits
for a full report. Stale handles cannot acquire new credit. PEER identity routing
uses OMQ's checked `IdentitySocket` view and explicit destination identities.

Metadata or payload can be empty but their frames are present. Bound all
three sizes before command decoding. There is no JSON on this path.

PEER adds OMQ routing metadata. No separate writer-data cookie is needed.
Endpoint scheme selects OMQ transport compression; it does not change this envelope.

| Offset | Width | Envelope field |
| ---: | ---: | --- |
| 0 | 4 | Magic `OZY` followed by zero |
| 4 | 1 | Version, `1` |
| 5 | 1 | Opcode |
| 6 | 2 | Flags; bit 0 response, others zero |
| 8 | 16 | Request ID; zero only for uncorrelated notifications |
| 24 | 16 | Sender node ID |
| 40 | 16 | Negotiated link session ID; zero for HELLO, PREPARE_PUB, and RECORDS_PUB |
| 56 | 4 | Metadata frame length |
| 60 | 4 | Payload frame length |

All integers are unsigned network byte order. `u128` IDs use canonical UUID
bytes, not machine-endian struct memory. `digest` is 32 bytes. `text` and
`blob` have a `u32` byte length followed by exact bytes; text is validated
UTF-8. `list<T>` has a `u32` item count followed by complete items.
`optional<T>` is one tag byte (`0` absent, `1` present) followed by T if present.
Reject any other tag, trailing bytes, overflow, duplicate mandatory fields,
and unsupported flags. Empty-prefix positions use optional values.

Schemas below list metadata fields in wire order. Nested records use the
listed order without implicit alignment. Length arithmetic is checked before
allocation. Diagnostics never determine retry or authority behavior.

### Handshake and sessions

`HELLO` metadata is `(instance_id: u128, hello_nonce: u128, properties)`.
`WELCOME` metadata is `(instance_id: u128, echoed_hello_nonce: u128,
properties)`; its envelope contains the fresh listener-selected session ID.
Subsequent requests and responses use that session. A response names a request
ID and sets response flag. Ordinary replies repeat their request ID; streaming
confirmation ranges can cover several requests in that session.

`HELLO` cannot carry application payload.
`HELLO` is a correlated request with a nonzero request ID and no response flag.
Its session is absent; `WELCOME` is a correlated response with a nonzero session.
An explicitly supplied zero ID is invalid, not a second spelling of absence.

Properties use RFC 24/26 length-prefixed names/values: `u8 name_length`, ASCII
name, `u32 value_length`, opaque value. Reject case-insensitive duplicates.
These target properties are required unless stated otherwise:

| Property | Value encoding |
| --- | --- |
| `versions` | List of supported `u8` majors; WELCOME selects one |
| `capabilities` | Counted sorted unique `u16` capability IDs |
| `required-capabilities` | Same encoding; selected set must contain all |
| `max-metadata-bytes` | `u32` |
| `max-payload-bytes` | `u32` |
| `max-record-bytes` | `u32`, total payload across one record's parts |
| `max-batch-records` | `u32` |
| `max-payload-parts` | `u32` |
| `max-inflight-records` | `u64` |
| `max-inflight-bytes` | `u64` |
| `roles` | `u32` bit set: producer, consumer, owner, replica, directory |

Capability IDs initially reserve 1 owner append, 2 confirmed progress, 3
durable inbox, 4 durable VSR, 6 snapshot transfer, 7 directory state,
8 groups, 9 replica receipt/credit flow, 10 owner authority routing, 11 reader
delivery/credit and volatile processing observations, 13 streaming owner
append. Capabilities 5, 12, and 14 are unassigned in the selected contract.
Capability 13 (`OWNER_STREAM`) requires capability 1 and selects record-based
retry and range confirmation semantics. The configured policy remains explicit.
Advertising a role does not authorize it. Unknown optional properties are
ignored; unknown required capabilities fail negotiation.

Receive limits are directional and retain the receiver's advertised values.
WELCOME must also advertise responder limits, not ambiguously replace both
directions with one window. Credit starts only after operation-specific open.

Link states are `Disconnected -> Negotiating -> Established -> Closing`.
Application commands in Negotiating fail with a bounded protocol error.
On transport/session replacement, invalidate old correlation/credit state,
repeat HELLO, reinstall desired subscriptions, and exchange durable progress.
Stable append identities survive; link request IDs and grants do not.

Writer sessions use an initiator/listener profile. `frontend::LinkSessions`
supports simultaneous initiation and multiplexed broker/client profiles through
the same HELLO/WELCOME state machine as embedded Node peers. Production IDs are
random; simulations inject distinct startup namespaces. The connection owner
checks configured identities and roles before negotiation. Negotiation supplies
link fences, never partition authority or destination credit. Disconnect retains
bounded peer metadata and rejects a delayed duplicate HELLO from that attempt.
The adapter must also fence old dispatcher grants and queued replies.

Metadata starts with the two 16-byte IDs, followed by properties until
frame end, without an outer property count. Counted major lists use `u32`
count and `u8` entries; encoders currently offer/select only 1.

Decoders bound properties to 32 and capability entries to 64, reject all
case-insensitive duplicate names, and ignore unknown optional properties and
capabilities. Required unknown capabilities fail. Receive bounds must be
nonzero, metadata capacity at least 512 bytes, and in-flight bounds at least one
maximum batch. Roles use bits 0..4 in the order listed above.

The native append profile advertises capability 1 for APPEND/APPENDED and
capability 10 for typed authority hints. Shared partition intake also accepts
OPEN_PRODUCER and returns PRODUCER_OPENED after canonical confirmation and
application. Its explicitly authorized writer identity is independent of the
shared broker connection. Existing embedded socket services still require
preprovisioned sessions. The low-level Connection requires capability 1; the
group-aware Client also requires 10 before sending an append.

Without capability 13, one outstanding append per client session is supported.
The streaming profile requires capabilities 1 and 13; the group-aware
Writer also requires capability 10. It advertises a bounded record count per APPEND and
independent directional receive windows, so sending does not wait for the
preceding confirmation. An ordinary append endpoint cannot silently
downgrade this profile.

The shared broker endpoint advertises append, streaming, routing, and reader
support. It does not require writer capabilities from a reader-only SDK.
Writers still require selected append/streaming capabilities. Partition intake
rejects APPEND without capability 13.

Duplicate HELLO with identical incarnation, nonce, and properties reuses its
session. A changed attempt selects a fresh session, discards obsolete outgoing
replies, and keeps admitted disk work charged until completion. Unknown,
pre-handshake, and stale-session traffic is discarded; clients may cancel and
renegotiate. Malformed established requests receive a bounded NACK where
possible. Application IDs survive caller-controlled reconnect/retry; the
low-level Connection has no fixed-timer payload retry.
`Connection::refresh_session` performs HELLO/WELCOME without resetting transport;
OMQ reconnects broken connections independently.

The group-aware Client registers the fixed configuration's three endpoints
without waiting for every voter. It refreshes sessions and follows only hints
matching that configuration and its deterministic primary selection. Views
never regress; repeated stale hints cannot prevent trying the other voters.
Only the request's transient view changes: owner/producer epochs, sequence,
record IDs, multipart bytes, and policy remain frozen. Each peer attempt has
separate handshake/response deadlines (defaults 1/2 seconds). An unanswered
append has unknown outcome. Exhausting all voters incurs exponential backoff
(50 ms initially, capped at 1 second); no per-record steady-state resend timer
is added. Caller cancellation stops waiting, not admitted server work.

For simultaneous Node initiation, lower node ID's HELLO wins. The higher ID
abandons its pending HELLO and includes its nonce in the optional WELCOME
property `superseded-hello` (nonzero `u128`). Node attempts increase monotonically
within a process. Each peer retains its highest observed/superseded attempt;
older HELLOs cannot reset a newer session, even across repeated reconnects.
Duplicate HELLO receives the same session and superseded nonce. HELLO cannot
carry this property. A later fresh HELLO can establish a replacement session.

Node session replacement closes old reader queues and pending response waits,
and removes old remote subscriptions. Subscription generations are installed
in receive order before bounded replay tasks start. Duplicate identical
SUBSCRIBE preserves active state; changing its selector/start requires a fresh
generation. Queues, sessions, subscriptions, and replay tasks are bounded.
Clients must be trusted or independently authenticated; routing IDs are not
credentials.

### Common records

`Authority = (group_id: u128, config_epoch: u64, view: u64)`.
`Partition = (incarnation: u128)` is the compact immutable handle resolved by
topic metadata. In the selected model it identifies a shared topic partition,
not a producer-owned log. Moving a partition preserves incarnation. Current
creation/state codecs still include producer-local identity and need correction.

`AppendKey = (producer_id: u128, producer_epoch: u64, first_sequence: u64)`.
`Position = optional<u64>`. `Grant = (revision: u64, record_limit: u64,
byte_limit: u64)` contains cumulative absolute allowances for this session.
`OpPosition = (op: u64, digest: digest)`. Group ops start at 1; op 0 and a
fixed zero digest denote the configured genesis prefix. Partition offsets
still start at zero. `Policy: u8` assigns 1 volatile, 2 buffered, 3 local
durable, 5 disk quorum, 6 retained RAM quorum with background persistence
(`QuorumReplicatedPersisting`). Value 4 is unassigned and rejected. The group
configuration binds its policy; a request cannot change it.

`Records = (count: u32, descriptors[count])`, where a descriptor is
`(message_id: u128, tagged_part_count: u32, optional_decoded_bytes: u32, lengths: u32[])`.
The count's low 24 bits count wire parts; its high byte is codec 0 (raw) or 1
(LZ4). Only codec 1 carries `decoded_bytes`, the original payload byte count.
Unknown codecs are rejected. Raw descriptors retain their existing layout.
The payload frame is the exact concatenation of all parts in descriptor order. Sum of lengths must equal
payload frame size. Empty parts remain distinct from absent parts. First
offset/sequence plus record index determines each record's position.

An LZ4 record has one wire part: `original_part_count:u32`, original lengths
(`u32` each), then one LZ4 block of concatenated original payload bytes. The
part table stays uncompressed. Brokers check its sum, decoded-size claim, and
part/record limits without decompression. Consumers require exactly that output
size and restore original parts. Retries retain the same encoding and bytes.

Producer APPEND uses only raw record descriptors and places compression outside
the record table. Immediately after policy it carries
`payload_codec:u8, decoded_payload_bytes:u32`. Codec 0 is raw and requires the
payload frame length to equal the decoded count. Codec 1 is one raw LZ4 block
over the concatenation of every described record part and must be nonempty.

The SDK attempts LZ4 at 2 KiB and sends it only when the block plus codec
metadata is smaller than raw. Brokers do not depend on this choice: after retry
checks, the leader may encode a fresh raw APPEND before assigning its canonical
digest.

The envelope supplies encoded length. Brokers validate clear descriptors, LZ4
syntax, exact decoded length, and complete input consumption without allocating
decoded output. Disk-group canonical bodies retain the clear descriptors and
exact encoded block.

### Command allocation

| Opcodes (hex) | Commands in ascending order |
| --- | --- |
| `01..03` | HELLO, WELCOME, ERROR |
| `10..17` | APPEND, APPENDED, LOOKUP_APPEND, APPEND_STATE, OPEN_PRODUCER, PRODUCER_OPENED, ACK_RESULTS, RESULTS_ACKED |
| `20..29` | SUBSCRIBE, SUBSCRIBED, RECORDS, ACK, PROGRESS_COMMIT, PROGRESS_COMMITTED, unassigned, UNSUBSCRIBE, UNSUBSCRIBED, CREDIT |
| `30..3f` | REPLICA_OPEN, REPLICA_STATE, PREPARE, PREPARE_OK, COMMIT, START_VIEW_CHANGE, DO_VIEW_CHANGE, START_VIEW, RECOVERY, RECOVERY_STATE, FETCH_OPS, OPS, SNAPSHOT_BEGIN, SNAPSHOT_CHUNK, SNAPSHOT_END, SNAPSHOT_INSTALLED |
| `40..43` | STATE_SNAPSHOT_REQUEST, STATE_SNAPSHOT, STATE_UPDATE, STATE_RESYNC |
| `50..53` | EXIT_VIEW, PREPARE_FLOW, PREPARE_PUB, RECORDS_PUB |
| `7f` | NACK |

Reserved ranges do not imply all commands are implemented together. Only
negotiated capability families can be used. Unknown opcodes are errors.

### Topic lookup

`STATE_SNAPSHOT_REQUEST` and `STATE_SNAPSHOT` with metadata tag `u8=0` carry
bounded topic pages over the established client PEER session. The request has
name length `u8`, ASCII topic name, first numeric partition `u32`, and maximum
entries `u16`. The correlated response has persistent topic ID `16`, the name,
hash algorithm `u8=1` (XXH3-64 modulo the fixed partition count in numeric
order), seed `u64`, total count `u32`, first partition `u32`, confirmation
policy `u8`, and broker count `u8`. Each broker entry has node ID `16` and
three `u16`-length UTF-8 endpoints: shared PEER, reader PUB, and optional
follower PUB (zero length means absent). A page ends with entry count `u16` and
contiguous entries of number `u32`, group ID `16`, configuration epoch `u64`,
incarnation `16`, ordered member count `u8`, and member IDs `16` each.

The broker shrinks a requested page to fit its control reply and negotiated
metadata limit. Clients advance by the returned entry count and compare all
repeated topic fields across pages. The broker never exposes local shard
placement. Payload frames are empty.

### Routing interests

The `40..43` watch profile uses PEER and a negotiated session. It carries
leader hints, not authority or confirmation. A client registers configured
group IDs on each reachable broker. Registration and snapshot capture run in
one dispatcher turn. Views compare only within the same group, partition
incarnation, configuration epoch, and ordered membership. A new view may change
the leader. An equal-view conflict between known leaders is invalid. An unknown leader may
become known within the same view. Session replacement drops old interests.

| Command | Metadata after tag `u8=1` | Envelope request ID |
| --- | --- | --- |
| `STATE_SNAPSHOT_REQUEST` | watch ID `16`, group count `u16`, group IDs `16` each | required request |
| `STATE_SNAPSHOT` | watch ID `16`, route count `u16`, route entries | required response |
| `STATE_UPDATE` | watch ID `16`, one route entry | absent notification |
| `STATE_RESYNC` | watch ID `16` | absent notification |

A route entry is group ID `16`, configuration epoch `u64`, partition incarnation
`16`, ordered member count `u8`, member IDs `16` each, view `u64`, and leader ID
`16`. A zero leader means no known leader. Configuration epochs are nonzero.
The codec accepts one, three, or six members. Six is
reserved without defining quorum behavior. Metadata and entry counts use fixed
receive limits. Payload frames are empty. Updates for one watched partition
coalesce. Overflow sends `STATE_RESYNC`; the client requests another snapshot.
Lost notices also require a fresh snapshot after timeout or wrong-leader reply.
The SDK validates group identity, configuration epoch, and membership against
trusted topic metadata before comparing views or using a hint.

### Producer exchange

- `OPEN_PRODUCER`: group tag `u8=1`, authority (group ID, configuration epoch,
  view), partition incarnation, producer ID, mode `u8` (`0` resume/create,
  `1` fence), expected epoch `u64` (zero means absent), operation ID `16`.
  Fencing requires an expected epoch. Retries keep the operation ID.
- `PRODUCER_OPENED`: same tag, authority, partition and producer, confirmed
  epoch, next sequence, retained retry floor, configured policy. A new epoch
  is committed before reply.
- `APPEND` group form: tag `u8=1`, authority, partition, expected owner epoch
  `u64`, append key, policy, payload codec, decoded payload bytes, records.
  Payload carries raw packed bytes or one group LZ4 block.
- `APPEND` local form: tag `u8=0`, stream:text, topic:text, local partition:u32,
  owner epoch:u64, append key, policy, payload codec, decoded payload bytes,
  records. Only local policies are valid.
- `APPENDED` without capability 13: tag `u8=1`, authority, partition, owner epoch, append
  key, first offset `u64`, count `u32`, achieved policy, op position, list of
  message IDs. Capability 13 selects the range layout below instead.
- `LOOKUP_APPEND`: authority, partition, append key, count, request digest.
- `APPEND_STATE`: status `u8`, followed by APPENDED fields for committed,
  op position for pending, or bounded typed NACK detail for expired/fenced.
- `ACK_RESULTS`: authority, partition, producer ID/epoch, expected and new
  exclusive result sequence floors, operation UUID.
- `RESULTS_ACKED`: authority, partition, producer epoch, committed exclusive
  result floor, operation position.

APPEND count is nonzero and policy must match the group's configured policy.
No silent fallback. All retries compare canonical payload and multipart
identity. The envelope request ID can change after reconnect while append key
stays stable. Session open/fence and assignment operations are idempotent too.

`ACK_RESULTS` is itself a replicated idempotent operation. Only its committed
floor permits retry identities/payloads below that sequence to expire. Opening
a newer producer epoch fences all old-epoch retry queries. A future configured
time horizon advances the same canonical floor; local wall-clock deletion never
bypasses it.

Group APPEND has 99 fixed metadata bytes including codec, decoded payload bytes,
and record count, followed by `20 + 4 * part_count` descriptor bytes per record.
Without capability 13, APPENDED has 146 fixed metadata bytes plus 16 bytes per
message ID; its two counts must agree. Neither count includes the 64-byte
envelope or OMQ routing metadata.

Requests carry no caller-assigned offsets/timestamps. Every record has at least
one part; empty parts remain valid. Sequence/offset ranges need a representable
exclusive end. Nonempty receipts cannot name genesis or a zero operation digest.
Encoders reject insufficient caller capacity without changing either frame;
decoders validate lengths before exposing borrowed iterators. Decoding an
APPENDED packet alone establishes no commit, session, or request authority.

#### Streaming writer profile

The implemented capability 13 profile reuses APPEND's envelope, metadata, and
multipart encoding, with one to the negotiated maximum records per request
(bounded by negotiated request and in-flight limits). The SDK additionally caps
one APPEND at 2,048 records. Sequence numbers advance
per record. The writer may fill the advertised in-flight record/byte windows
without waiting for a reply. OMQ batches transport work; the broker groups
ready records across writers and partitions for replication without waiting
for a batch to fill. Neither grouping determines retry identity.

Group streaming APPENDED has 106 metadata bytes and an empty payload frame:

| Offset | Width | Field |
| ---: | ---: | --- |
| 0 | 1 | Group tag, 1 |
| 1 | 88 | Authority, partition, owner epoch, append key |
| 89 | 8 | Exclusive confirmed sequence end |
| 97 | 8 | First confirmed partition offset |
| 105 | 1 | Achieved policy |

Local streaming APPENDED replaces tag/group authority/partition with tag 0,
stream:text, topic:text, and local partition:u32. Owner epoch, append key,
exclusive sequence end, first offset, and achieved policy follow unchanged.

The append key identifies the writer, producer epoch, and first confirmed
sequence. The range is nonempty; both sequence and offset ranges must have
representable exclusive ends. There is no message-ID list or operation proof.
The envelope is a correlated APPENDED response in the active session. Its
request ID names an outstanding APPEND, not necessarily the range's last
record; the typed sequence range determines exactly which records it confirms.

Each range covers one writer's partition, owner fence, producer epoch, and
policy. Broker groups may produce separate ranges for several writers; one
writer's range may span several groups. Ranges may overlap an already confirmed
prefix, but cannot leave a gap or cover records not validated in this session.
The writer checks the sender, session, correlation, authority, all identities,
sent sequence bounds, and stable epoch-to-offset mapping before completion.

Disk-confirmed replies use `QuorumDurable` only after an applied synchronized
group prefix. `QuorumReplicatedPersisting` replies require applied history
retained by the leader and one follower; local disk completion is independent.
Local replies name volatile retention, complete buffered write, or local sync.
Transport admission and broker-local acceptance are not confirmation.

Reconnect discards transient correlation and resends unresolved records with
unchanged identities, message IDs, multipart boundaries, and bytes. Retry
lookup compares records across old group boundaries. Confirmed matches return
their original offsets; accepted-only matches wait for confirmation without
appending another copy. Conflicts and expired retry history remain explicit
errors. A replacement session starts a new validated range, never a claim on
unverified records elsewhere in the broker's confirmed history.

### Writer transport

SDK writers send APPENDs over the same PEER connection as HELLO/WELCOME,
control, NACKs, and APPENDED confirmations. There is no separate data endpoint,
cookie, or PUSH fallback. Sparse traffic sends single-record APPENDs on this
same path. Adaptive LZ4 and APPEND metadata are independent of socket type.

Routing identity, sender, session, opcode, and request correlation must match.
Producer access, destination, policy, record/byte credit, and retry identity
retain their ordinary APPEND checks. Each request occupies its slot until its
last record is confirmed. Flush waits for its captured confirmed sequence end;
socket ordering or transport admission never substitutes for confirmation.
Retry unresolved records unchanged after session replacement.

#### SDK protocol batching

The SDK always uses APPEND's counted record table and
streaming APPENDED ranges for requests containing one or more
consecutive records from one writer/epoch/partition and policy. No new command
or wire major is required by this decision. Callers still submit single records;
[Runtime](RUNTIME.md#sdk-protocol-batching) owns collection and scheduling.

Advertise and honor negotiated `max-batch-records` together with metadata,
payload, part, and in-flight limits. A peer advertising one record still receives
one; capability 13 alone is not permission to exceed that limit. Sequence numbers
advance per record. Request IDs identify attempts, not retry units or atomic
application transactions. Ranges may confirm part of a request or span requests,
but must pass all existing session, authority, sent-record, and offset checks.

Local and group intake validates the complete request before admitting its
records. A request may span several internal storage/replication groups and receive
partial confirmations; its correlation remains live until its last record is
confirmed. Packet boundaries never determine retry identity.

### Reader exchange
Capability 11 implements exact-offset subscriptions, pushed records, cumulative
credit, cancellation, and volatile receive/processing observations. It does not
advertise durable inbox, durable progress, or consumer groups.

`Subscription = (id:u128, generation:u128)`; both are nonzero. Generation is
opaque. Every replacement uses a fresh generation. Link session, subscription
generation, and source fence every delivery, credit grant, and observation.

`Target` selects the requested partition:

- Tag 0: `(stream:text, topic:text, local_partition:u32)`.
- Tag 1: `(Authority, incarnation:u128, owner_epoch:u64)`.

`Source` identifies the accepted log. Group source has the same tag-1 fields.
Local source is tag 0 followed by `(producer_id:u128, local_partition:u32)`;
its topic is fixed by the subscription and its producer comes from the owner's
registered log. Local storage never invents a group or replication evidence.

| Command | Metadata fields |
| --- | --- |
| SUBSCRIBE | Subscription, Target, exact start:u64 |
| SUBSCRIBED | Subscription, Source |
| RECORDS | Subscription, Source, first offset:u64, payload codec:u8, decoded payload bytes:u32, Records |
| CREDIT | Subscription, Source, cumulative records:u64, cumulative bytes:u64 |
| ACK | Subscription, Source, received:optional<u64>, processed:optional<u64> |
| UNSUBSCRIBE / UNSUBSCRIBED | Subscription, Source |

SUBSCRIBE/SUBSCRIBED and UNSUBSCRIBE/UNSUBSCRIBED are correlated exchanges.
An identical repeated SUBSCRIBE preserves its state; changed parameters require
a new generation. Stream/topic names are nonempty UTF-8, at most 255 bytes each.
A reader starting at zero receives an explicit gap if zero has expired.

RECORDS uses APPEND's outer raw/LZ4 payload codec and validated record
descriptors. A leader forwards a stored producer LZ4 block unchanged, one block
per RECORDS message, and waits for credit rather than split that batch. It sends
raw records only when a read starts inside a batch or the negotiated window can
never hold the whole batch.

Native push deliveries are uncorrelated notifications: no request ID or
response flag.
Each message contains whole, nonempty, contiguous records, never beyond applied
confirmation. The initial seek is indexed; one persistent cursor continues
through both existing history and new appends. At the end it parks until a
confirmed append, source change, credit grant, or buffer release wakes it.
There is no FETCH command, repeated history polling, or replay/live handoff.

Credit grants are cumulative record and payload-byte totals for one generation.
The shared broker path echoes accepted CREDIT in a correlated CREDIT response.
The SDK retries refusals until this response, then stops retrying that grant.
The response proves only subscription-capacity acceptance. It never confirms
records, durability, or application processing.
Only released receiving capacity permits a larger grant. Duplicate/stale grants
cannot add capacity. Both outstanding records and bytes stay within negotiated
windows. A record must fit wholly; zero byte credit admits only empty payloads.
A retained transport clone keeps its output arena unavailable until actual drop.
This is distinct from reader credit, writer confirmation, and processing ACKs.

ACK positions are inclusive; processed cannot exceed received. Notifications
have no request ID or response flag. Observation never grants capacity, confirms
an append, or commits durable application progress. Legacy Node delivery also
accepts correlated record receipts; the native subscription path does not wait
for one receipt per output message.
Shared SDK processing observations use a correlated ACK exchange. Its response
echoes accepted positions and does not make them durable.

Group requests fence group/configuration/view, partition, and owner epoch.
The group-aware reader follows leader hints and resumes after its last delivered
record with a fresh generation. An explicitly addressed normal follower can
serve its own confirmed history, without claiming the group's latest state.
Receive progress is volatile. Applications requiring durable processing must
track it themselves and tolerate replay.

Future offsets receive NACK 16. Expired offsets receive NACK 14 with the earliest
retained offset. An individually oversized record receives NACK 17. None splits
a record, silently seeks, or makes a slow reader block writer confirmation.
Readers pin no retained history and consume no writer proposal permits.

PROGRESS_COMMIT/PROGRESS_COMMITTED and group assignment remain reserved.
Capability 12 and opcode `0x26` are unassigned.

#### Live reader publication

A group leader can publish confirmed records once for every live reader of a
partition, on a PUB socket of its own. The subscription exchange above stays the
path for history, gap repair, and control.

| Frame | Contents |
| --- | --- |
| 0 | Topic: group ID (16 bytes), then partition incarnation (16 bytes) |
| 1 | `RECORDS_PUB` (`0x53`) envelope, no link session or request ID |
| 2 | Source, first offset:u64, payload codec:u8, decoded payload bytes:u32, record descriptors |
| 3 | Packed raw payload or one exact producer LZ4 block, shared where possible |

Frames 2 and 3 equal RECORDS without its Subscription. A reader that subscribes
to the bare group ID receives every partition of that group. Only group sources
are published; local logs have no live stream.

Publish only confirmed and applied records, in offset order. PUB never waits: a
slow reader loses messages and never delays writers or other readers. Queue
bounds count messages and bytes. Receipt proves neither authority nor
completeness: readers compare the Source with their SUBSCRIBED source and check
offsets. Whoever may connect to this endpoint may read every partition it
publishes; deployments that need isolation per partition leave it unbound.

A reader subscribes to the topic before its first SUBSCRIBE, then follows one
of two states:

| State | Delivers | Replay subscription |
| --- | --- | --- |
| Replay | SUBSCRIBE at the next offset, under credit | Open |
| Live | Publications only | Canceled with UNSUBSCRIBE |

- A publication that starts at or before the next offset ends replay, even if
  all of its records were delivered before: the live stream now covers the
  cursor. Covered records are dropped.
- A publication that starts later is held, and live reads pause. Replay runs
  from the next offset with its record credit capped at the held first offset,
  then the held publication is delivered. Further loss shows as the next gap.
- After a quiet interval without live progress the reader returns to replay.
  This finds a lost final publication and a changed leader. It stays in replay
  until a publication arrives without a gap.
- A publication from a newer source releases held state and restarts replay;
  SUBSCRIBE follows leader hints as before. Older sources are dropped.
- Expired history during repair stays an explicit NACK 14. Nothing is skipped.

### Replica exchange

PEER replica messages start with an 80-byte prefix: Authority, configured voter
ID (`u128`), configuration digest (`32 bytes`). The authenticated peer must
match both the envelope sender and embedded voter; the established session and
complete configuration scope must match too. Construction of `PeerBinding`
asserts independent authentication; decoding never establishes it.
Essential remaining metadata:

| Command | Fields after common replica prefix |
| --- | --- |
| REPLICA_OPEN | Outstanding tail `OpPosition`, available operation `u64`; nonzero request ID in envelope |
| REPLICA_STATE | Receive epoch `u128`, revision `u64`, base/received `OpPosition`, received body bytes `u64`, absolute operation/body limits `u64` each, repair limit `u64` |
| PREPARE | First op, predecessor digest, commit position, list of canonical operation descriptors |
| PREPARE_FLOW | Receive epoch `u128`, then the same fields as PREPARE |
| OPS | Source descriptor, predecessor op position, list of canonical operation descriptors |
| PREPARE_OK | Contiguous op position, evidence `u8` (2 durable, 3 retained RAM with background persistence; 1 is unassigned), grant |
| COMMIT | Committed op position |
| EXIT_VIEW | View to leave in Authority: the sender's current view, or an older view it left, answering a peer's request; volatile timeout suspicion, no additional fields |
| START_VIEW_CHANGE | Proposed view in Authority; no additional fields |
| DO_VIEW_CHANGE | Frozen writer generation `u128`, last normal view `u64`, accepted end, commit floor |
| START_VIEW | Installed writer generation `u128`, selected accepted end, commit floor |
| RECOVERY | Fresh recovery-attempt nonce `u128`; correlated request ID in envelope |
| RECOVERY_STATE | Echoed nonce `u128`, primary tag `u8`; if primary: writer generation `u128`, accepted and commit `OpPosition` |
| FETCH_OPS | Source descriptor, predecessor op position, maximum operations `u32`, maximum canonical body bytes `u32` |

PREPARE/OPS payload is the concatenation of canonical operation bodies in
descriptor order. Each descriptor is `(op:u64, original_view:u64, kind:u16,
canonical_body_bytes:u32, body_digest, op_digest)`. Derive and validate
consecutive op numbers. Original view
is retained when a prepare is carried into a newer active Authority view.
The wire and disk encoders share the canonical operation schema, not file
padding, physical group numbers, or consumer subscription envelopes.

PREPARE, PREPARE_OK, COMMIT, EXIT_VIEW, and the three election messages are uncorrelated notifications:
response flag clear, request ID absent. ACKs are cumulative, not one response
per prepare. Their payload frame is empty except PREPARE. ACK metadata is 145
bytes; COMMIT is 120. The journal-backed codec checks evidence against the
immutable group policy; retained RAM evidence cannot count as a disk vote.

PREPARE metadata is `164 + 86 * operation_count` bytes, including the common
prefix and `u32` count. Initial maximum is 64 operations, independently of
records per operation. Zero counts, nonconsecutive ops, unsupported kinds,
trailing bytes, and body/hash-chain mismatches are rejected. Op zero pairs
only with the zero digest; `u64::MAX` is not an admissible operation position.

Encoding reuses independently verified body digests. Decode hashes each body
once and returns borrowed operations without a per-operation vector. Typed
body/application validation and role/view/commit checks remain mandatory
before admission. These codecs live in `ozzy-replication`, since canonical
journal bodies already depend on `ozzy-proto`; reversing that edge would cycle.

#### Live replica publication

Journal-backed groups can send live operations over PUB/SUB. Each SUB socket is
bound to an independently configured broker endpoint. PEER retains confirmations,
flow probes, elections, and targeted repair.

| Frame | Contents |
| --- | --- |
| 0 | Group ID (16 bytes), the common SUB topic |
| 1 | `PREPARE_PUB` (`0x52`) envelope, no link session or request ID |
| 2 | PREPARE metadata, including leader view and configuration digest |
| 3 | Shared canonical operation bodies |

Publish each fresh group once, without recipient masks or follower credit checks.
Check the bound publisher, current leader/view, group, and configuration before
hashing bodies. Followers admit only a contiguous, capacity-bounded suffix.

On a gap or full admission window, hold one shared publication and pause SUB
reads. PEER repair and journal work continue. Resume after predecessors or space
arrive; discard duplicates. Scope/generation changes and receive resets discard
held state. Same-view queued bodies can be revalidated after a reset; they carry
no receive-epoch credit or confirmation authority.

PEER repair keeps epoch-bound credits. A PUB receipt beyond PEER reservations
requires independent local-history verification before advancing its cursor.
Periodic correlated probes detect loss of the final publication too. Neither
publication nor volatile receipt is a quorum vote.

#### Receipt and credit flow

A held SUB publication bounds PEER repair at its predecessor. While live receipt
advances, the receiver instead caps repair at its received prefix. After live
progress stops for the configured quiet interval, tail repair is unrestricted.
The optional operation-number limit occupies bytes 208..216 of REPLICA_STATE; `u64::MAX`
means unrestricted. Only a correlated response can set the repair bound.
It neither grants credit nor confirms history.

Capability 9 selects the [receipt/credit contract](REPLICATION.md#receipt-credit-and-repair), independently
of durable vote evidence. REPLICA_OPEN is a correlated, empty-payload request;
its metadata is 128 bytes. The outstanding tail at byte 80 exposes a lost last
payload even when no later PREPARE arrives. The available-operation number at
byte 120 includes unsent work awaiting credit. It cannot extend the repair tail
or prove confirmation. Only an activated normal receiver may advertise credits;
an opening request cannot bypass VSR recovery.

REPLICA_STATE has 216 metadata bytes and an empty payload. It either echoes a
probe's nonzero request ID with the response flag set, or is an unsolicited
same-epoch update with both absent. Fields after the 80-byte common prefix:
receive epoch at 80, revision at 96, fixed epoch base at 104, received prefix at
144, cumulative received body bytes at 184, operation limit at 192, byte limit
at 200. Epoch/revision are nonzero. Counters count unique canonical operations
and uncompressed canonical body bytes after the fixed base, not records or
compressed wire bytes. This is not the existing record-based `Grant` type.
Receipt carries no durability or application evidence.

An epoch change requires a response matching the live probe's scope/request ID,
authenticated peer/session, and verified local history. Shape decoding alone
does not authorize it. Same-epoch revisions and exact prefix/byte checks belong
to the bounded flow ledger. A credit window can exceed one wire payload; codecs
check structural fields without mistaking frame bounds for negotiated credit
capacity. The sender applies its independent ledger bounds before admission.

PREPARE_FLOW uses opcode `0x51`, `180 + 86 * operation_count` metadata bytes,
and the same canonical payload/descriptors as PREPARE. Its only additional
field is a 16-byte receive epoch immediately after the common prefix. It is an
uncorrelated notification. `PeerBinding::with_receive_epoch` requires an exact
locally issued epoch before body decoding/hashing and rejects legacy PREPARE.
A binding without an epoch rejects PREPARE_FLOW. Epochs are not learned from
incoming payload fields. COMMIT, PREPARE_OK, and installation transfer retain
their own unchanged authority checks and layouts.

The distinct opcode avoids silently extending the existing PREPARE layout.
Credit-aware actors must require the complete flow family; no silent fallback
to unlimited-grant legacy sends. The trusted-static actor uses finite credits
from REPLICA_STATE and leaves the legacy PREPARE_OK record grants at zero.
Negotiated capability/session support remains a
separate integration gate; merely recognizing these opcodes does not supply it.

Election metadata is fixed-size: START_VIEW_CHANGE 80 bytes, DO_VIEW_CHANGE
184, START_VIEW 176. These schemas require complete retained WAL history; they
do not negotiate checkpoint transfer. Generations are nonzero big-endian
`u128` writer incarnations, not local file offsets or commit evidence. Pin the
reported generation and accepted tail until the report is invalidated. Fetch
missing operations separately; never substitute missing bytes with an empty log.

The proposed view must be nonzero and exceed a report's last normal view.
Commit cannot exceed accepted or use a different digest at the same position.

EXIT_VIEW also has 80 metadata bytes and an empty payload; view zero is valid.
It requests departure from exactly the named current view. It neither promises
a higher view nor contributes to START_VIEW_CHANGE/DO_VIEW_CHANGE quorums.
The timed driver requires this command and durable election commands together.
Do not mix it with older unilateral-timeout drivers; compatible capability
negotiation remains a runtime integration gate, not implied by opcode decoding.
Decode establishes none of the required durable promise, publication, or
new-view quorum boundaries. The production core still enforces those gates.
Checkpoint-backed election descriptors need an explicitly negotiated schema
before they can replace the full-WAL requirement.

`Source = (voter_id:u128, writer_generation:u128, accepted:OpPosition)` binds
history requests and responses to a pinned full-WAL source. FETCH_OPS is a
correlated request (response flag clear); OPS echoes its nonzero request ID
with the response flag set. Their common voter is the actual packet sender,
not necessarily the source named in a request. An OPS sender must be that
source. FETCH_OPS metadata is 200 bytes with an empty payload; OPS metadata is
`196 + 86 * operation_count` bytes. OPS carries no commit announcement.

The serving adapter checks source ownership and keeps that exact generation
pinned while reading. Requested count/body-byte bounds are nonzero upper limits,
not allocation instructions; the source also applies its own smaller work and
wire limits. Empty responses and requests already at/beyond the tail are invalid.
Responses must match the live request's scope, ID, source, predecessor, and
limits. Invalidate pending exchanges when their role/session ends. Validated
chunks preserve original views and concatenate into one chain; only reaching
the advertised tail digest validates the complete selected lineage. Neither
hash validation nor a received chunk proves local disk installation or commit.

#### Lost-state recovery

RECOVERY has 96 metadata bytes: the common 80-byte replica prefix and a nonzero
16-byte attempt nonce. Its view is a hint, never a promise. The envelope carries
an independently nonzero request ID with the response flag clear. RECOVERY_STATE
echoes that request ID with the response flag set and the nonce at offset 80.
Its common scope carries the responder's actual normal view, which can differ
from the hint. Both payload frames are empty.

Only activated normal voters respond. Tag 0 at offset 96 identifies a backup,
ending its metadata at 97 bytes. Tag 1 identifies the configured primary of the
response view; generation at 97, accepted prefix at 113, and commit prefix at
153 end its metadata at 193 bytes. Other tags or mismatched configured roles are
invalid. Generation is nonzero. Commit cannot exceed accepted or differ at the
same position. These full-WAL schemas contain no checkpoint descriptor and
provide neither durable vote evidence nor authorization to resume voting.

The nonce is globally fresh per logical recovery attempt and stable across
reconnects; session/request IDs independently fence each outstanding exchange.
The core needs fresh evidence from both other voters, including the highest
observed normal view's primary. Its snapshot includes every accepted operation,
even unsynced/uncommitted prepares that an old in-flight ACK can still commit.
Capture that prefix once; asynchronously settle and pin its exact bytes. Cache
the descriptor immutably per nonce/view, not per transient link request. Never
recapture a moving accepted tail to answer a retry or wait for all later writes
to become quiescent. Fetch its history through the source-bound OPS protocol.

Complete canonical validation, crash-safe replacement publication, and fenced
election handoff are separate gates. Codec support alone advertises no recovery
capability; the runtime must not respond until those adapters exist.

`TransferDescriptor = (transfer_id:u128, base:OpPosition, end:OpPosition,
bytes:u64, digest, chunk_bytes:u32)`. Checkpoint descriptors add immutable
manifest ID and retained-range metadata digest. Missing chunks cannot be
interpreted as an empty suffix. Normal acceptance waits for a complete
validated view installation.

Snapshot commands share replica prefix and transfer ID. BEGIN carries the
descriptor and required disk bytes. CHUNK carries file ID `u64`, byte offset
`u64`, uncompressed length `u32`, codec `u8`, and digest; bytes are payload.
END carries manifest digest. INSTALLED carries installed checkpoint/op and
generation. Canonical manifest encodes file IDs, sizes/digests, group/config,
checkpoint, and retained partition ranges; receivers generate local paths.

Use no compression initially (`codec = 0`). Future compression must bound
uncompressed bytes before decoding and verify canonical digests afterward.
Logical op hashes do not depend on a negotiated transport compression choice.

### Directory exchange and errors

Directory messages carry directory incarnation, request/snapshot ID, and
revision. Snapshot request includes an optional cached revision; snapshot
response uses the bounded transfer descriptor. Update includes previous and
new revision plus counted typed map changes. Resync supplies the current
incarnation/revision. Exact directory value schemas are frozen with its
implementation, under the directory capability; no opaque JSON assumption.

ERROR/NACK metadata is `(code:u16, retry_class:u8, detail:blob, diagnostic:text)`.
Retry classes: permanent, after-credit, after-reconnect, after-authority-refresh,
unknown-outcome. Codes distinguish malformed encoding, unsupported capability,
identity rejection, stale session, stale view/owner/producer epoch, sequence
gap/conflict, retention gap, result expired, quota exhaustion, storage failure,
and unavailable quorum. Detail schemas are code-specific and bounded; no
string parsing. Unknown codes remain typed unknown errors with raw detail.

The NACK codec freezes retry classes as 1..5 in the order above. Fixed metadata
overhead is 11 bytes; payload is empty. The native append service uses these
initial codes, extended by reader commands, with empty diagnostics:

| Code | Meaning | Retry class |
| ---: | --- | --- |
| 1 | Malformed or oversized command | permanent |
| 2 | Unsupported command or policy | permanent |
| 3 | Source/producer identity not authorized | permanent |
| 5 | Stale/foreign group, configuration, or view | after-authority-refresh |
| 6 | Unknown partition or fenced owner/producer | permanent |
| 7 | Sequence gap | permanent |
| 8 | Retry identity conflicts with retained records | permanent |
| 9 | Retry history expired | permanent |
| 10 | Capacity unavailable or earlier writer sequences missing | after-credit |
| 11 | Storage/validation uncertainty | unknown-outcome |
| 12 | Writer not admitted by current primary, or reader broker unavailable | after-authority-refresh |
| 13 | Authority changed after admission | unknown-outcome |
| 14 | Reader offset/retention gap | permanent |
| 15 | Unknown or replaced subscription generation | permanent |
| 16 | Requested offset beyond local confirmed end | permanent |
| 17 | Next indivisible record exceeds read bounds | permanent |
| 18 | Unknown topic, metadata partition, or routing-watch group | permanent |

With capability 10 selected, codes 5, 12, and 13 require exactly 48 detail bytes:
`(group_id:u128, config_epoch:u64, observed_view:u64, primary_id:u128)`. Group,
configuration epoch, and primary ID are nonzero; view zero is valid. The view
can name an unfinished election. Clients validate group/configuration and
configured primary selection after response session/correlation checks. A hint
never proves primary activation or commit. Without capability 10 the service
emits no authority detail.

Native subscriptions use code 14 detail `(earliest_offset:u64)`,
code 16 detail `(committed_end:u64)`, and code 17 detail
`(payload_bytes:u64, parts:u64)`. These fields use network byte order and exact
lengths. Other implemented errors have empty detail. Legacy Node subscription
errors may lack position detail; native readers require exact typed positions.

Code 4 is reserved for explicit stale-session rejection; the initial actor drops
stale-session packets instead. An error about this attempt never rules out an
earlier successful attempt with the same append identity. Without capability
13, a busy per-client slot retains its original pending request/reply and
ignores additional requests until released; the low-level client exposes only
one active request at a time. Streaming instead bounds each writer's queued
and unconfirmed records/bytes independently. Exhausting that window returns
code 10 and stops further admission on that session; reconnect reconciles the
unresolved records. One writer's blocked reply does not stop other writers.

Never expose prepared data or claim a failed append solely because its reply
timed out. Protocol deadlines are local API policy, not cancellation of a
committed operation.

### Limits and compatibility gate

Starting data limits are 64 KiB metadata, 16 MiB packet payload, 1 MiB per record,
1,000 records per batch,
and 64 KiB bulk-transfer chunks. They are configuration candidates to measure.
The admitted record/operation size must fit every required replica's negotiated
limits including descriptors; minimum-limit membership cannot be bypassed by
fragmenting an atomic application operation invisibly.

Decode/decompression and command scheduling have independent count/byte
budgets. Reserved control capacity does not bypass the order of bytes already
queued on TCP; see [RUNTIME.md](RUNTIME.md).

Exact wire layouts have fixed-byte tests, including malformed lengths,
unsupported versions, source/correlation/session fences, and capacity failures.
The removed 40-byte prototype envelope is rejected by shape. No transparent
fallback or alternate protocol mode is supported.
