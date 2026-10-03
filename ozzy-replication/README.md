# ozzy-replication

Sans-I/O replication core. Fixed three-voter normal operation, bounded
prepare pipeline, canonical digest checks, generation-scoped disk completion,
durable quorum commit, and ordered application. No runtime or transport deps.

`flow` supplies separate, preallocated sender/receiver ledgers for volatile
receipts and absolute canonical-operation/body credits. Report revisions, exact
prefix/byte checks, and receive epochs prevent credit duplication or implicit
state retraction. `Transmitter` combines these checks with bounded small probes,
correlated epoch adoption, and range repair. The actor uses the same policy for
live/catch-up sends, as does the lifecycle simulator. No receipt counts as a
durable vote; see [replica flow contract](../doc/REPLICATION.md#receipt-credit-and-repair).

`ConfigurationRecord` encodes the complete supported fixed configuration:
ordered voters, principal fingerprints, disk-quorum policy, and protocol/schema
versions. Its domain-separated digest supplies core/wire scope. Journal adapters
can bind its exact immutable bytes; the transport must still authenticate peers.
See [configuration binding](../doc/REPLICATION.md#configuration-and-authority).

The normal core starts at explicit empty-group bootstrap or completed selected
view installation. Fencing stops
ACKs and commit immediately while allowing outstanding disk work to settle.
View-change initiation consumes that fenced role, waits for old journal work,
and requires durable view-promise completion before election messages. It
collects distinct first/second-phase voters, selects by last-normal-view then
accepted prefix, and verifies protected prefixes against pinned history.
Selection does not grant leadership. Installation consumes the old role; exact
durable publication and rebuilt committed application state activate the new one.
A newer view observed during I/O suppresses intermediate normal activation.
Primary fresh appends wait for a new-view quorum and selected-tail application.
Intact-disk restart leaves the last installed normal view or resumes a higher
durably promised but never installed election. Repeated restarts cannot ratchet
that unfinished view. It retains accepted history and known commit floors,
without restoring ACK sets or old normal authority. Storage must independently validate
the complete log and immutable configuration first. Authenticated deployment
orchestration remains separate from the recovery core. The fixed-voter OMQ
actor composes catch-up, installation, and activation on bounded disk workers;
three-process tests cover primary loss, intact rejoin, and full-cluster restart.

`recovery::Recovery` adds the nonvoting lost-state admission core. It requires
fresh nonce-matched responses from both other normal voters, including the
highest observed view's primary. Transfer retains the primary's full accepted
prefix, including unsynced/uncommitted operations that a pre-crash ACK may still
commit. Full-WAL validation is count/byte-bounded with no retained descriptors.
Exact durable publication and private application validation return only disk
state for the existing fenced election path, never same-view normal authority.
Newer views, conflicting frozen responses, and stale callbacks cannot release
recovery. Tests include a 40-operation history with a one-operation live limit.
The runtime journal worker owns immutable donor pins; the segment store has a
nonvoting configuration publication gate. Receiving-worker/actor orchestration
uses dedicated bounded workers; see [recovery](../doc/REPLICATION.md#restart-and-recovery).

`driver::ReplicaDriver` owns normal-to-election scheduling: primary contact and
commit-progress deadlines, idle COMMIT heartbeats, bounded first/second-phase
retransmission, increasing election timeouts, and explicit durable promises.
It emits one action per poll and preserves pending disk work across later views.
Timeouts send volatile EXIT_VIEW requests; two current-view requests authorize
departure. A lone deaf voter cannot ratchet the view. Durable START_VIEW_CHANGE
instead fences older authority immediately, allowing intact restart to rejoin.
Exit requests never count as durable start/report votes. Exact promises,
start/report quorums, and selected-history installation remain required.
The adapter supplies monotonic time, authenticated messages, actual completions,
and pinned history lookup. The driver owns pending primary/backup installation,
retains later-view fences until publication settles, and retransmits immutable
START_VIEW alongside COMMIT. Installation I/O and ACK grants remain adapter work.

`begin_validation` captures a voter/writer/view and exact accepted/committed/applied
image before asynchronous application validation. `prepare_validated` rejects a
stale completion before admission. Tickets carry no validation or durability proof;
the worker must validate the exact operations against the matching image. Changed
authority or application progress requires discarding the plan and validating again.

`wire` implements native PREPARE, durable PREPARE_OK, COMMIT, EXIT_VIEW, START_VIEW_CHANGE,
DO_VIEW_CHANGE, START_VIEW, RECOVERY, RECOVERY_STATE, FETCH_OPS, and OPS. Encoding
uses caller-owned buffers and cached body digests; decoding validates bounded
hash chains and borrows bodies. Independent authenticated session/configuration
binding precedes decode; typed application validation and core authority checks
still precede admission. Election descriptors bind a nonzero frozen writer
generation and complete accepted/commit history; they require retained full WAL,
not checkpoint transfer. History transfer matches the live request, source
generation, tail, predecessor, and count/byte bounds without commit authority.
Recovery replies bind both the current link request and a separate fresh attempt
nonce, so reconnects can retain the same immutable donor snapshot. Configured
primary roles and complete accepted/commit descriptors are checked on decode.
This does not enable recovery service in the actor or advertise a capability.
REPLICA_OPEN/REPLICA_STATE encode correlated probes and coalesced volatile
receipts/credits. PREPARE_FLOW adds a receiver-issued epoch; epoch-bound decoding
rejects legacy/stale data before payload hashing. The static actor requires this
family and finite credits. Other command bodies and session
negotiation are pending.

Restart descriptors do not load the journal into the live pipeline. Streamed
installation validates count/byte-bounded chunks and retains only the tail
anchor. Whole-tail quorum and application release fresh appends. Intermediate
tail evidence needs journal lookup; it is never inferred from an array slot.
Real-journal tests compose bounded multi-segment staging, private canonical
replay, and one whole-tail activation commit publication. Historical operations
do not consume the live application pipeline. Native Node integration remains pending.

Adapters must validate canonical operations and application transitions before
admission, authenticate voter identities, retain payloads, and report actual
write/sync completion. Neither network receipt nor page-cache write is a
durable vote. See [replication contract](../doc/REPLICATION.md).

```sh
cargo test -p ozzy-replication
```
