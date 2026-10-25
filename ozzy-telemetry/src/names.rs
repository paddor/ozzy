//! Stage and event names, in published order.

/// How one event's values from different threads combine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// Occurrences or amounts. Totals add.
    Count,
    /// The largest observed value. Totals keep the largest.
    Peak,
    /// A level that any thread raises or lowers. Totals add with wrapping, so
    /// one thread's own value can look negative.
    Gauge,
}

impl Kind {
    /// The kind of the event published as `name`.
    pub fn of(name: &str) -> Option<Self> {
        let index = EVENT_NAMES.iter().position(|event| *event == name)?;
        Some(EVENT_KINDS[index])
    }

    /// A total combined with one more thread's value.
    pub fn merge(self, total: u64, value: u64) -> u64 {
        match self {
            Self::Count | Self::Gauge => total.wrapping_add(value),
            Self::Peak => total.max(value),
        }
    }
}

// Variant indices and published labels come from one ordered declaration.
macro_rules! counters {
    (
        $(#[$enum_doc:meta])* $enum:ident, $names:ident {
            $($(#[$variant_doc:meta])* $variant:ident => $label:literal),* $(,)?
        }
    ) => {
        $(#[$enum_doc])*
        #[derive(Clone, Copy, Debug)]
        #[repr(usize)]
        pub enum $enum {
            $($(#[$variant_doc])* $variant),*
        }
        pub(crate) const $names: [&str; [$($label),*].len()] = [$($label),*];
    };
}

macro_rules! kind {
    () => {
        Kind::Count
    };
    ($kind:ident) => {
        Kind::$kind
    };
}

// Events also declare how threads' values combine; unmarked events count.
macro_rules! events {
    (
        $(#[$enum_doc:meta])* $enum:ident, $names:ident, $kinds:ident {
            $($(#[$variant_doc:meta])* $variant:ident => $label:literal $(as $kind:ident)?),*
            $(,)?
        }
    ) => {
        counters! {
            $(#[$enum_doc])* $enum, $names {
                $($(#[$variant_doc])* $variant => $label),*
            }
        }
        pub(crate) const $kinds: [Kind; $names.len()] = [$(kind!($($kind)?)),*];

        impl $enum {
            /// How this event's values from different threads combine.
            pub const fn kind(self) -> Kind {
                $kinds[self as usize]
            }
        }
    };
}

counters! {
    /// Measured local stages; transport transit between processes is excluded.
    Stage, STAGE_NAMES {
        /// SDK admission to request packing, per record transmission attempt.
        SdkQueue => "sdk_queue",
        /// Received confirmation decoding and SDK prefix publication, per reply.
        ConfirmationApply => "confirmation_apply",
        /// OMQ admission to complete APPEND confirmation, per request.
        RequestRoundtrip => "request_roundtrip",
        /// Source progress RPC attempt through its response, including failures.
        ProcessingRoundtrip => "processing_roundtrip",
        /// Native proposal admission through observed completion, including failures.
        NativeProposal => "native_proposal",
        /// Storage admission through execution start on its backend worker.
        IoQueue => "io_queue",
        /// Backend worker execution through physical completion, including failures.
        IoExecution => "io_execution",
        /// Proposal submission to the replication actor starting validation.
        ReplicaQueue => "replica_proposal_queue",
        /// Leader-local admission completion to the successful proposal reply.
        /// Includes waiting for another broker and applying the confirmed prefix.
        ReplicaConfirmation => "replica_confirmation",
        /// Local actor proposal submission through validation completion.
        LocalPropose => "local_propose",
        /// Local actor admission submission through its journal completion.
        LocalAdmit => "local_admit",
        /// Local actor admission through observed physical write completion.
        LocalWrite => "local_write",
        /// Local actor barrier submission through durable evidence publication.
        LocalSyncPublish => "local_sync_publish",
        /// Durable evidence publication through local driver installation.
        LocalSyncInstall => "local_sync_install",
        /// Local actor apply submission through journal completion.
        LocalApply => "local_apply",
        /// Physical data-write future, excluding journal scheduling and installation.
        JournalPhysicalWrite => "journal_physical_write",
        /// Detached data barrier and durability evidence publication future.
        JournalPhysicalSync => "journal_physical_sync",
        /// Reader request admission through observed journal completion, before encoding.
        ReaderJournal => "reader_journal",
        /// Resident or historical delivery encoded into one reader reply.
        ReaderEncode => "reader_encode",
        /// SDK materializes one buffered live or replayed record, excluding socket wait.
        ReaderMaterialize => "reader_materialize",
        /// Whole-APPEND LZ4 attempt in SDK preparation runtime.
        SdkCompression => "sdk_compression",
        /// Broker shard output enqueue through dispatcher reply admission.
        ShardReplyQueue => "shard_reply_queue",
        /// Unselected storage cleanup pass while a retention turn holds the journal.
        RetentionCleanup => "retention_cleanup",
    }
}

events! {
    /// Opt-in replication events. Counts describe transport policy, never votes.
    Event, EVENT_NAMES, EVENT_KINDS {
        /// Cumulative processing intent observed by its SDK owner.
        ProcessingReady => "processing_ready",
        /// Source progress RPC started by its SDK owner.
        ProcessingRequest => "processing_request",
        /// Source progress response observed by its SDK owner.
        ProcessingConfirmed => "processing_confirmed",
        /// Source progress request or leader selection needs another attempt.
        ProcessingRetry => "processing_retry",
        /// Source progress RPC decoded by the broker intake.
        ProcessingReceived => "processing_received",
        /// Progress input held because all consumer request slots are occupied.
        ProcessingSlotRefusal => "processing_slot_refusal",
        /// Progress preparation held because charged control backing is exhausted.
        ProcessingArenaRefusal => "processing_arena_refusal",
        /// Group activation input held because consumer request slots are occupied.
        AssignmentSlotRefusal => "assignment_slot_refusal",
        /// Group activation preparation held because charged control backing is exhausted.
        AssignmentArenaRefusal => "assignment_arena_refusal",
        /// Writer input held because its request slots are occupied.
        NativeSlotRefusal => "native_slot_refusal",
        /// Writer preparation held because charged arena backing is exhausted.
        NativeArenaRefusal => "native_arena_refusal",
        /// Native proposal successfully entered its existing actor lane.
        NativeProposalSubmitted => "native_proposal_submitted",
        /// Native proposal completed at its configured confirmation boundary.
        NativeProposalCommitted => "native_proposal_committed",
        /// Native proposal completed without confirmed admission.
        NativeProposalUnconfirmed => "native_proposal_unconfirmed",
        /// Native proposal completion observed; distinguish source progress from APPEND.
        ProcessingCompleted => "processing_completed",
        /// Group activation proposal completion observed.
        AssignmentCompleted => "assignment_completed",
        /// Storage job began executing on its backend worker.
        IoStarted => "io_started",
        /// Storage job began on reserved progress-class execution capacity.
        IoProgressStarted => "io_progress_started",
        /// Worker began a physical read operation.
        IoRead => "io_read",
        /// Worker began a physical write operation.
        IoWrite => "io_write",
        /// Worker began a file or directory durability barrier.
        IoSync => "io_sync",
        /// Backend execution completed successfully; canceled observation still counts.
        IoCompleted => "io_completed",
        /// Backend execution completed with an error other than a probe miss.
        IoFailed => "io_failed",
        /// An open, metadata, remove, or directory creation found the path
        /// absent or present: existence probes, mostly at startup.
        IoProbeMissed => "io_probe_missed",
        /// Successful physical write bytes, counted by the execution worker.
        IoWriteBytes => "io_write_bytes",
        /// Successful physical read bytes, counted by the execution worker.
        IoReadBytes => "io_read_bytes",
        /// Fresh data packet reserved and queued to one follower.
        ReplicaSend => "replica_send",
        /// Data packet queued using an existing repair reservation.
        ReplicaRepair => "replica_repair",
        /// Validated packet reached the follower's normal receive policy.
        ReplicaReceive => "replica_receive",
        /// Packet skipped because its predecessor is ahead of the retained prefix.
        ReplicaReceiveGap => "replica_receive_gap",
        /// Packet skipped because retained, unreleased bodies fill the receive ledger.
        ReplicaReceiveLedgerFull => "replica_receive_ledger_full",
        /// Packet skipped because accepted, validating, and queued suffixes fill the
        /// follower's two live windows, or it lies beyond the leader's window above
        /// the announced commit.
        ReplicaReceiveWindowFull => "replica_receive_window_full",
        /// Packet skipped because the queued arena or its payload memory cannot hold it.
        ReplicaReceiveArenaFull => "replica_receive_arena_full",
        /// Packet was already present in the retained receive prefix.
        ReplicaReceiveDuplicate => "replica_receive_duplicate",
        /// Correlated status query queued to one peer, including retries.
        ReplicaProbe => "replica_probe",
        /// Worker result rejected because its application/authority ticket changed.
        ReplicaStaleValidation => "replica_stale_validation",
        /// Recent replay lookup has no entries in this authority generation.
        ReplayCacheEmpty => "replay_cache_empty",
        /// Applied packets no longer form a contiguous history in the same image.
        ReplayCacheReset => "replay_cache_reset",
        /// Requested predecessor has fallen out of the bounded packet cache.
        ReplayCacheEvicted => "replay_cache_evicted",
        /// Requested predecessor is inside a retained multi-operation packet.
        ReplayCacheInterior => "replay_cache_interior",
        /// Requested predecessor is beyond the retained packet range.
        ReplayCacheAhead => "replay_cache_ahead",
        /// Recent packet found, but send range or flow metadata cannot serve it.
        ReplayPacketMiss => "replay_packet_miss",
        /// Recent packets served replay without reserving journal work.
        ReplayCacheHit => "replay_cache_hit",
        /// Applied control packet bypassed the optional replay cache.
        ReplayCacheControlBypass => "replay_cache_control_bypass",
        /// Oversized backing bypassed the optional replay cache.
        ReplayCacheOversizedBypass => "replay_cache_oversized_bypass",
        /// Cached packet removed to satisfy the operation or backing-byte bound.
        ReplayCacheCapacityEviction => "replay_cache_capacity_eviction",
        /// Cold replay waits for pending persistence to settle, per actor turn.
        ReplayPersistenceWait => "replay_persistence_wait",
        /// Cold replication fetch submitted to the journal owner.
        ReplayDiskFetch => "replay_disk_fetch",
        /// One live publication queued for the group.
        ReplicaPublication => "replica_publication",
        /// Publication passed leader/scope/integrity checks.
        ReplicaPublicationReceived => "replica_publication_received",
        /// Canonical bytes submitted for targeted PEER replication, including retries.
        ReplicaPayloadBytes => "replica_payload_bytes",
        /// Canonical bytes inspected by receive staging, including held-message retries.
        ReceiveBytes => "receive_bytes",
        /// Canonical bytes in packets wholly covered by the receive cursor.
        DuplicateBytes => "duplicate_bytes",
        /// Canonical bytes newly retained by receive staging.
        RetainedBytes => "retained_bytes",
        /// One publication of confirmed records queued for every live reader.
        ReaderPublication => "reader_publication",
        /// SDK whole-APPEND LZ4 attempts.
        SdkCompressionAttempt => "sdk_compression_attempt",
        /// SDK attempts retained as LZ4.
        SdkCompressionWin => "sdk_compression_win",
        /// SDK attempts retained raw.
        SdkCompressionRaw => "sdk_compression_raw",
        /// Raw bytes presented to SDK LZ4 attempts.
        SdkCompressionInputBytes => "sdk_compression_input_bytes",
        /// Candidate bytes produced by SDK LZ4 attempts.
        SdkCompressionOutputBytes => "sdk_compression_output_bytes",
        /// Journal-owner whole-APPEND LZ4 attempts.
        BrokerCompressionAttempt => "broker_compression_attempt",
        /// Journal-owner attempts retained as LZ4.
        BrokerCompressionWin => "broker_compression_win",
        /// Journal-owner attempts retained raw.
        BrokerCompressionRaw => "broker_compression_raw",
        /// Raw bytes presented to journal-owner LZ4 attempts.
        BrokerCompressionInputBytes => "broker_compression_input_bytes",
        /// Candidate bytes produced by journal-owner LZ4 attempts.
        BrokerCompressionOutputBytes => "broker_compression_output_bytes",
        /// Most live operations awaiting application or persistence.
        LiveWindowPeakOperations => "live_window_peak_operations" as Peak,
        /// Most live body bytes awaiting application or persistence.
        LiveWindowPeakBytes => "live_window_peak_bytes" as Peak,
        /// Leader proposals held back until the live window had room.
        LiveWindowFull => "live_window_full",
        /// SDK APPEND refused until broker admission capacity becomes available.
        SdkAdmissionRefusal => "sdk_admission_refusal",
        /// Native APPEND proposal lane refused an otherwise prepared request.
        NativeProposalRefusal => "native_proposal_refusal",
        /// Dispatcher reply queue refused a shard-owned frame.
        ShardReplyRefusal => "shard_reply_refusal",
        /// Local command lane refused a shard-owned reply.
        ShardPortRefusal => "shard_port_refusal",
        /// Same-channel 29-byte receipt report queued to PEER.
        CompactReceipt => "compact_receipt",
        /// Full history/session binding or correlated probe response.
        FullReceipt => "full_receipt",
        /// Partition actor answered a writer request with an admission refusal.
        NativeAdmissionRefusal => "native_admission_refusal",
        /// Dequeued client input discarded without a reply: stale or ignored.
        ShardInputDiscarded => "shard_input_discarded",
        /// One shard scheduler poll (a wakeup of the shard thread).
        ShardTurn => "shard_turn",
        /// A safety-sweep visit found work no wakeup or timer announced.
        PartitionSweepProgress => "partition_sweep_progress",
        /// Follower PUB socket was full; the frame waits for space.
        ReplicaPublicationSendWait => "replica_publication_send_wait",
        /// Reader PUB socket was full; the frame waits for space.
        ReaderPublicationSendWait => "reader_publication_send_wait",
        /// Follower PUB frame could not enter its bounded shard queue.
        ReplicaPublicationQueueDrop => "replica_publication_queue_drop",
        /// Follower PUB frame arrived beyond the contiguous retained prefix.
        ReplicaPublicationGap => "replica_publication_gap",
        /// Follower PUB admission refused a SUBSCRIBE of an unconfigured peer.
        FollowerSubscriptionRejected => "follower_subscription_rejected",
        /// Reader PUB admission refused a SUBSCRIBE: not a live topic the
        /// SUB's node holds a subscription to.
        ReaderSubscriptionRejected => "reader_subscription_rejected",
        /// Reader PUB connection closed: it held a live topic after its node's
        /// subscription ended.
        ReaderSubscriptionRevoked => "reader_subscription_revoked",
        /// Reader PUB monitor lost events; each connection it knew is closed.
        ReaderSubscriptionLag => "reader_subscription_lag",
        /// Leader validation check found its journal owner running a command,
        /// rolling, or holding unobserved completions. Counted per check.
        ValidationOwnerWait => "validation_owner_wait",
        /// Leader validation check waited for payload memory. Counted per check.
        ValidationMemoryWait => "validation_memory_wait",
        /// Leader validation that may read files waited for earlier writes to
        /// settle. Counted per check.
        ValidationSettleWait => "validation_settle_wait",
        /// Follower validation waited for application to advance: its staged
        /// suffix needs deduplication capacity that the leader freed at a
        /// commit this replica has not applied yet. Counted per check.
        ValidationApplyWait => "validation_apply_wait",
        /// Validation check refused because the control-identity overlay is
        /// full until a settled index refresh. Counted per check.
        ValidationIdentityWait => "validation_identity_wait",
        /// Journal command held until writes settle because the writeback
        /// backlog had no room. Counted per submission attempt.
        JournalCapacityWait => "journal_capacity_wait",
        /// Journal command that needs settled writes held behind physical
        /// work. Counted per submission attempt.
        JournalSettleWait => "journal_settle_wait",
        /// Leader proposal deferred until its duplicate's accepted-only
        /// original is applied. Later proposals queue behind it.
        OriginalWait => "original_wait",
        /// Bounded retention turn given to the journal once foreground writes
        /// settled: record retention, identity expiry, or checkpoint work.
        RetentionTurn => "retention_turn",
        /// Bounded identity-expiry step run between foreground turns without
        /// waiting for a settled journal.
        IdentityRetention => "identity_retention",
        /// Record retention turn found nothing to do or had to wait: the
        /// journal was unsettled or out of append capacity.
        RetentionDeferred => "retention_deferred",
        /// Record retention turn waited for an earlier retirement to install.
        RetentionRetiring => "retention_retiring",
        /// Retention proposed a record or producer-result floor.
        RetentionFloor => "retention_floor",
        /// Retention captured one sealed segment's retirement checkpoint.
        RetirementStarted => "retirement_started",
        /// A retirement checkpoint was selected and its prefix unreferenced.
        RetirementInstalled => "retirement_installed",
        /// Unselected storage objects removed by retention cleanup.
        StorageCleanupRemoved => "storage_cleanup_removed",
        /// Leader proposal deferred for identity or payload-memory capacity.
        CapacityDeferral => "capacity_deferral",
        /// Shard command applied by the dispatcher: a reply, publication, or
        /// route update.
        DispatcherCommand => "dispatcher_command",
        /// Queued dispatcher reply handed to its OMQ destination socket.
        DispatcherReplySent => "dispatcher_reply_sent",
        /// Queued dispatcher reply refused by a full OMQ client socket.
        DispatcherReplyBlocked => "dispatcher_reply_blocked",
        /// Queued dispatcher frame refused by a full OMQ broker socket.
        DispatcherBrokerBlocked => "dispatcher_broker_blocked",
        /// Dispatcher queue for a client refused a shard frame.
        DispatcherReplyFull => "dispatcher_reply_full",
        /// Shard reply refused because an earlier reply to the same
        /// destination was refused and is not yet re-admitted.
        DispatcherReplyHeld => "dispatcher_reply_held",
        /// Dispatcher queue for a broker refused a shard frame.
        DispatcherBrokerFull => "dispatcher_broker_full",
        /// Received frame the dispatcher rejected without retaining it.
        DispatcherInputDropped => "dispatcher_input_dropped",
        /// Received frame returned to its socket because its shard lane was full.
        DispatcherInputPaused => "dispatcher_input_paused",
        /// Paused source retried after its shard lane reported space.
        DispatcherInputRetry => "dispatcher_input_retry",
        /// Send-progress wait completed for a refused destination.
        DispatcherSendWake => "dispatcher_send_wake",
        /// SDK writer saw no APPEND response before its deadline and retries.
        SdkResponseTimeout => "sdk_response_timeout",
        /// HELLO dropped because the broker already admits `limits.max_clients`.
        ClientLimitRefusal => "client_limit_refusal",
        /// Source progress update request decoded by the broker, including
        /// retries and refusals.
        ProgressUpdateRequest => "progress_update_request",
        /// Source progress query decoded by the broker, including retries and
        /// refusals.
        ProgressQueryRequest => "progress_query_request",
        /// Operation batch selected from resident active or predecessor journal
        /// data for a reply.
        ResidentSelection => "resident_selection",
        /// Reply batch selected from accepted records still in the writeback queue.
        BackgroundResidentRead => "background_resident_read",
        /// Native group reply encoded directly from shared RAM selections.
        DirectResidentRead => "direct_resident_read",
        /// Native group reply assembled by a historical reader worker.
        HistoricalReadDelivery => "historical_read_delivery",
        /// Reader payload bytes shared with existing immutable backing.
        SharedReaderBytes => "shared_reader_bytes",
        /// Reader payload bytes copied into the independent transport buffer.
        CopiedReaderBytes => "copied_reader_bytes",
        /// Local canonical APPEND operation built, including repeated preparation.
        PreparedOperation => "prepared_operation",
        /// Largest local canonical APPEND payload.
        PreparedPayloadBytesMax => "prepared_payload_bytes_max" as Peak,
        /// Encoded operations, group seals, and alignment padding appended to a
        /// journal. Excludes file headers, indexes, manifests, failed writes,
        /// and unused capacity: logical bytes, not device write amplification.
        EncodedGroupBytes => "encoded_group_bytes",
        /// Dedup index image installed, including recovery rebuilds.
        DedupIndexImage => "dedup_index_image",
        /// Table, queue, and scratch bytes reserved by installed dedup images.
        DedupIndexBytes => "dedup_index_bytes",
        /// Identity capacity configured by installed dedup images.
        DedupIndexEntries => "dedup_index_entries",
        /// Bytes reserved by live installed dedup images.
        DedupLiveBytes => "dedup_live_bytes" as Gauge,
        /// Identity capacity configured by live installed dedup images.
        DedupLiveEntries => "dedup_live_entries" as Gauge,
    }
}
