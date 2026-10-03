use ozzy_journal::operation::{
    Barrier, CanonicalOperation, OperationBody, OperationLimits, canonical_body_digest,
    encode_operation_body,
};
use ozzy_proto::{GroupId, LinkSessionId, NodeId, OperationId};
use ozzy_replication::wire::{
    Control, PeerBinding, ReplicaMessage, WireError, WireLimits, decode, encode_control,
};
use ozzy_replication::{
    Admission, Configuration, Digest, DoViewChange, FrozenLog, InstallOutcome, JournalGeneration,
    NormalReplica, PipelineLimits, Prefix, PreparedOperation, StartView, StartViewChange,
    ViewChange, ViewChangeError,
};

fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index + 1; 16])
}

fn configuration() -> Configuration {
    Configuration::new(
        GroupId::from_bytes([7; 16]),
        1,
        Digest::from_bytes([8; 32]),
        [node(0), node(1), node(2)],
    )
    .unwrap()
}

fn session() -> LinkSessionId {
    LinkSessionId::from_bytes([9; 16])
}

fn normal(index: u8) -> NormalReplica {
    NormalReplica::bootstrap(
        configuration(),
        node(index),
        JournalGeneration(u128::from(index) + 1),
        PipelineLimits {
            max_operations: 8,
            max_body_bytes: 8192,
        },
    )
    .unwrap()
}

fn operation() -> PreparedOperation {
    let body = OperationBody::Barrier(Barrier {
        operation_id: OperationId::from_bytes([10; 16]),
    });
    let bytes = encode_operation_body(&body, OperationLimits::default()).unwrap();
    PreparedOperation::from_verified(
        &CanonicalOperation {
            group_id: configuration().scope().group_id,
            configuration_epoch: 1,
            original_view: 0,
            op_number: 1,
            previous_digest: Digest::ZERO,
            kind: body.kind(),
            body: &bytes,
        },
        canonical_body_digest(&bytes),
    )
}

fn durable(index: u8) -> NormalReplica {
    let mut core = normal(index);
    let Admission::Write { ticket, .. } = core
        .prepare(node(0), configuration().scope(), &[operation()])
        .unwrap()
    else {
        panic!("fresh append");
    };
    core.complete_durable_write(ticket).unwrap();
    core
}

fn promised(core: NormalReplica, view: u64) -> ViewChange {
    let mut election = core.into_view_change(view).unwrap();
    let promise = election.begin_promise().unwrap();
    election.complete_promise(promise).unwrap();
    election
}

fn transmitted(from: u8, control: Control) -> Control {
    let mut metadata = [0; 184];
    let encoded = encode_control(node(from), session(), control, &mut metadata).unwrap();
    let ReplicaMessage::Control(decoded) = decode(
        &[&encoded.header, &metadata[..encoded.metadata_bytes], &[]],
        PeerBinding::new(configuration(), node(from), session()).unwrap(),
        WireLimits::default(),
    )
    .unwrap() else {
        panic!("election control");
    };
    assert_eq!(decoded, control);
    decoded
}

fn exchange_starts(left: &mut ViewChange, left_id: u8, right: &mut ViewChange, right_id: u8) {
    let Control::StartViewChange(start) = transmitted(
        left_id,
        Control::StartViewChange(left.start_message().unwrap()),
    ) else {
        panic!("start");
    };
    right.receive_start(node(left_id), start).unwrap();
    let Control::StartViewChange(start) = transmitted(
        right_id,
        Control::StartViewChange(right.start_message().unwrap()),
    ) else {
        panic!("start");
    };
    left.receive_start(node(right_id), start).unwrap();
}

#[test]
fn encoded_view_change_start_cannot_replace_the_receivers_durable_promise() {
    let mut source = normal(0).into_view_change(1).unwrap();
    assert_eq!(
        source.start_message(),
        Err(ViewChangeError::PromiseRequired)
    );
    let promise = source.begin_promise().unwrap();
    source.complete_promise(promise).unwrap();
    let control = Control::StartViewChange(source.start_message().unwrap());
    let mut metadata = [0xff; 184];
    let encoded = encode_control(node(0), session(), control, &mut metadata).unwrap();
    let mut golden = [0; 80];
    golden[..16].fill(7);
    golden[23] = 1;
    golden[31] = 1;
    golden[32..48].fill(1);
    golden[48..80].fill(8);
    assert_eq!(encoded.header[5], 0x35);
    assert_eq!(encoded.metadata_bytes, 80);
    assert_eq!(&metadata[..80], &golden);
    assert!(metadata[80..].iter().all(|&byte| byte == 0xff));
    let ReplicaMessage::Control(Control::StartViewChange(start)) = decode(
        &[&encoded.header, &golden, &[]],
        PeerBinding::new(configuration(), node(0), session()).unwrap(),
        WireLimits::default(),
    )
    .unwrap() else {
        panic!("start view change");
    };
    let mut receiver = normal(1).into_view_change(1).unwrap();
    receiver.receive_start(node(0), start).unwrap();
    receiver.receive_start(node(0), start).unwrap();
    assert_eq!(receiver.report(), Err(ViewChangeError::PromiseRequired));
    let promise = receiver.begin_promise().unwrap();
    receiver.complete_promise(promise).unwrap();
    assert_eq!(receiver.report().unwrap().scope, start.scope);
}

#[test]
fn encoded_report_preserves_acknowledged_history_without_commit_announcement() {
    let mut primary = durable(0);
    let backup = durable(1);
    primary
        .receive_ack(node(1), backup.acknowledgment().unwrap())
        .unwrap();
    let acknowledged = primary.snapshot().committed;
    primary.apply_through(acknowledged).unwrap();
    drop(primary); // Client succeeded; neither survivor received COMMIT.
    let mut source = promised(backup, 2);
    let mut candidate = promised(normal(2), 2);
    exchange_starts(&mut source, 1, &mut candidate, 2);
    candidate.report().unwrap();
    let report = source.report().unwrap();
    let mut metadata = [0xff; 200];
    let encoded = encode_control(
        node(1),
        session(),
        Control::DoViewChange(report),
        &mut metadata,
    )
    .unwrap();
    assert_eq!(encoded.header[5], 0x36);
    assert_eq!(encoded.metadata_bytes, 184);
    assert_eq!(&metadata[80..96], &2_u128.to_be_bytes());
    assert_eq!(&metadata[96..104], &0_u64.to_be_bytes());
    assert_eq!(&metadata[104..112], &1_u64.to_be_bytes());
    assert_eq!(&metadata[112..144], acknowledged.digest.as_bytes());
    assert_eq!(&metadata[144..184], &[0; 40]);
    assert!(metadata[184..].iter().all(|&byte| byte == 0xff));
    let Control::DoViewChange(report) = transmitted(1, Control::DoViewChange(report)) else {
        panic!("report");
    };
    candidate.receive_report(node(1), report).unwrap();
    candidate.receive_report(node(1), report).unwrap();
    let selected = candidate.select(|_, _| None).unwrap();
    assert_eq!(selected.source().voter, node(1));
    assert_eq!(selected.source().generation, report.generation);
    assert_eq!(selected.source().accepted, acknowledged);
    assert_eq!(selected.committed(), Prefix::GENESIS);
    assert!(!candidate.normal_snapshot().ready_for_appends);
}

#[test]
fn decoded_start_view_still_requires_installation_and_a_fresh_tail_quorum() {
    let mut candidate = promised(durable(2), 2);
    let mut backup = promised(durable(1), 2);
    exchange_starts(&mut candidate, 2, &mut backup, 1);
    candidate.report().unwrap();
    let Control::DoViewChange(report) =
        transmitted(1, Control::DoViewChange(backup.report().unwrap()))
    else {
        panic!("report");
    };
    candidate.receive_report(node(1), report).unwrap();
    candidate.select(|_, _| None).unwrap();
    let mut pending = candidate
        .install_primary(JournalGeneration(20), &[operation()])
        .unwrap();
    let ticket = pending.ticket();
    let InstallOutcome::Normal(mut primary) = pending.complete(ticket, Prefix::GENESIS).unwrap()
    else {
        panic!("installed primary");
    };
    assert!(!primary.snapshot().ready_for_appends);
    let start = primary.start_view().unwrap().unwrap();
    let mut metadata = [0xff; 184];
    let encoded =
        encode_control(node(2), session(), Control::StartView(start), &mut metadata).unwrap();
    assert_eq!(encoded.header[5], 0x37);
    assert_eq!(encoded.metadata_bytes, 176);
    assert_eq!(&metadata[80..96], &20_u128.to_be_bytes());
    assert_eq!(&metadata[96..104], &1_u64.to_be_bytes());
    assert_eq!(&metadata[104..136], operation().prefix().digest.as_bytes());
    assert_eq!(&metadata[136..176], &[0; 40]);
    assert!(metadata[176..].iter().all(|&byte| byte == 0xff));
    let Control::StartView(start) = transmitted(2, Control::StartView(start)) else {
        panic!("start view");
    };
    let mut pending = backup
        .install_backup(
            node(2),
            start,
            JournalGeneration(21),
            &[operation()],
            |_, _| None,
        )
        .unwrap();
    let ticket = pending.ticket();
    let InstallOutcome::Normal(backup) = pending.complete(ticket, Prefix::GENESIS).unwrap() else {
        panic!("installed backup");
    };
    primary
        .receive_ack(node(1), backup.acknowledgment().unwrap())
        .unwrap();
    assert_eq!(primary.snapshot().committed, operation().prefix());
    assert!(!primary.snapshot().ready_for_appends);
    primary.apply_through(operation().prefix()).unwrap();
    assert!(primary.snapshot().ready_for_appends);
}

fn election_controls() -> [Control; 3] {
    let mut scope = configuration().scope();
    scope.view = 2;
    let generation = JournalGeneration(20);
    let accepted = operation().prefix();
    [
        Control::StartViewChange(StartViewChange { scope }),
        Control::DoViewChange(DoViewChange {
            scope,
            generation,
            log: FrozenLog {
                last_normal_view: 0,
                accepted,
                committed: accepted,
            },
        }),
        Control::StartView(StartView {
            scope,
            generation,
            accepted,
            committed: accepted,
        }),
    ]
}

#[test]
fn election_packets_reject_truncation_extra_bytes_and_request_correlation() {
    let binding = PeerBinding::new(configuration(), node(2), session()).unwrap();
    for message in election_controls()
        .into_iter()
        .chain([Control::ExitView(configuration().scope())])
    {
        let mut metadata = [0; 185];
        let encoded = encode_control(node(2), session(), message, &mut metadata).unwrap();
        let size = encoded.metadata_bytes;
        let decoded = |header: &[u8], metadata: &[u8], payload: &[u8]| {
            decode(&[header, metadata, payload], binding, WireLimits::default()).map(|_| ())
        };
        for length in 0..size {
            let mut header = encoded.header;
            header[56..60].copy_from_slice(&(length as u32).to_be_bytes());
            assert!(decoded(&header, &metadata[..length], &[]).is_err());
        }
        let mut header = encoded.header;
        header[56..60].copy_from_slice(&(size as u32 + 1).to_be_bytes());
        assert_eq!(
            decoded(&header, &metadata[..=size], &[]),
            Err(WireError::Length)
        );
        let mut header = encoded.header;
        header[60..64].copy_from_slice(&1_u32.to_be_bytes());
        assert_eq!(
            decoded(&header, &metadata[..size], &[1]),
            Err(WireError::Payload)
        );
        let mut header = encoded.header;
        header[8] = 1; // Correlated notifications are not this protocol.
        assert_eq!(
            decoded(&header, &metadata[..size], &[]),
            Err(WireError::Correlation)
        );
        header[7] = 1;
        assert_eq!(
            decoded(&header, &metadata[..size], &[]),
            Err(WireError::Correlation)
        );
        let mut small = [0xaa; 184];
        assert_eq!(
            encode_control(node(2), session(), message, &mut small[..size - 1]),
            Err(WireError::Capacity)
        );
        assert_eq!(small, [0xaa; 184]);
    }
}

#[test]
fn exit_view_has_distinct_golden_bytes_and_allows_view_zero_without_a_promise() {
    let scope = configuration().scope();
    let mut metadata = [0xff; 81];
    let encoded =
        encode_control(node(0), session(), Control::ExitView(scope), &mut metadata).unwrap();
    let mut golden = [0; 80];
    golden[..16].fill(7);
    golden[23] = 1;
    golden[32..48].fill(1);
    golden[48..].fill(8);
    assert_eq!(encoded.header[5], 0x50);
    assert_eq!(encoded.metadata_bytes, 80);
    assert_eq!(&metadata[..80], &golden);
    assert_eq!(metadata[80], 0xff);
    assert_eq!(
        decode(
            &[&encoded.header, &golden, &[]],
            PeerBinding::new(configuration(), node(0), session()).unwrap(),
            WireLimits::default()
        )
        .unwrap(),
        ReplicaMessage::Control(Control::ExitView(scope))
    );
    let mut impersonated = encoded.header;
    impersonated[5] = 0x35;
    assert_eq!(
        decode(
            &[&impersonated, &golden, &[]],
            PeerBinding::new(configuration(), node(0), session()).unwrap(),
            WireLimits::default()
        ),
        Err(WireError::History)
    ); // View zero cannot be interpreted as a durable first-phase vote.
}

#[test]
fn impossible_frozen_histories_are_rejected_before_counting_a_vote() {
    let binding = PeerBinding::new(configuration(), node(2), session()).unwrap();
    for message in election_controls() {
        let mut metadata = [0; 184];
        let encoded = encode_control(node(2), session(), message, &mut metadata).unwrap();
        let size = encoded.metadata_bytes;
        let rejected = |bytes: &[u8]| {
            decode(
                &[&encoded.header, &bytes[..size], &[]],
                binding,
                WireLimits::default(),
            )
            .map(|_| ())
        };
        let mut invalid = metadata;
        invalid[24..32].fill(0);
        assert_eq!(rejected(&invalid), Err(WireError::History));
        if size == 80 {
            continue;
        }
        let mut invalid = metadata;
        invalid[80..96].fill(0);
        assert_eq!(rejected(&invalid), Err(WireError::History));
        let commit = size - 40;
        let mut invalid = metadata;
        invalid[commit + 7] = 2; // Commit outruns accepted op 1.
        assert_eq!(rejected(&invalid), Err(WireError::History));
        let mut invalid = metadata;
        invalid[commit + 8] ^= 1; // Equal position, different history.
        assert_eq!(rejected(&invalid), Err(WireError::History));
        if size == 184 {
            let mut invalid = metadata;
            invalid[103] = 2; // Last installed view must precede proposed view.
            assert_eq!(rejected(&invalid), Err(WireError::History));
        }
    }
    let Control::DoViewChange(mut report) = election_controls()[1] else {
        panic!("report");
    };
    report.generation = JournalGeneration(0);
    let mut output = [0xaa; 184];
    assert_eq!(
        encode_control(
            node(2),
            session(),
            Control::DoViewChange(report),
            &mut output
        ),
        Err(WireError::History)
    );
    assert_eq!(output, [0xaa; 184]);
}
