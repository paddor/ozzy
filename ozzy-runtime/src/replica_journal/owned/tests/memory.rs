use super::*;
use ozzy_journal::operation::OperationKind;

#[test]
fn journal_arenas_consume_reserved_shard_capacity_and_aliases_remain_charged() {
    let (mut controller, io) = setup();
    let (mut owner, _) = drive(
        &mut controller,
        OwnedJournal::format_new(
            config("/journal", 1, QuorumPolicy::Durable),
            io,
            JournalGeneration(1),
            32768,
        ),
    )
    .unwrap();
    let memory = payload_owner(64);
    let capacity = memory.capacity();
    owner.bind_append_capacity(&capacity).unwrap();
    let mut buffer = owner.lease_append_buffer().unwrap();
    assert!(!buffer.reserve_incoming(32).unwrap());
    assert!(buffer.read_bytes().is_empty());
    memory
        .reserve(
            &capacity,
            crate::memory::Quota {
                bytes: 64,
                buffers: 2,
            },
        )
        .unwrap();
    buffer.push_read_part(&[1; 32]).unwrap();
    let bytes = buffer.shared_bodies();
    buffer.clear();
    assert_eq!(
        capacity.remaining(),
        crate::memory::Quota {
            bytes: 32,
            buffers: 1
        }
    );
    buffer.push_read_part(&[2; 32]).unwrap();
    assert_eq!(capacity.remaining(), crate::memory::Quota::default());
    let second = buffer.shared_bodies();
    drop(buffer);
    drive(&mut controller, owner.shutdown()).unwrap();
    drop(capacity);
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 64);
    assert_eq!(bytes.as_ref(), &[1; 32]);
    assert_eq!(second.as_ref(), &[2; 32]);
    drop((bytes, second));
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 0);
}

#[test]
fn incoming_chunk_reserves_all_bodies_before_partial_staging() {
    let (mut controller, io) = setup();
    let (mut owner, _) = drive(
        &mut controller,
        OwnedJournal::format_new(
            config("/journal", 1, QuorumPolicy::Durable),
            io,
            JournalGeneration(1),
            32768,
        ),
    )
    .unwrap();
    let memory = payload_owner(32);
    owner.bind_append_memory(&memory).unwrap();
    let mut buffer = owner.lease_append_buffer().unwrap();
    buffer.push_read_part(&[1; 16]).unwrap();
    assert!(!buffer.reserve_incoming(16).unwrap());
    assert_eq!(buffer.read_bytes(), &[1; 16]);
    buffer.clear();
    assert!(buffer.reserve_incoming(32).unwrap());
    // Both copies use the same reserved arena, even with no byte headroom.
    buffer.push_read_part(&[2; 16]).unwrap();
    buffer.push_read_part(&[3; 16]).unwrap();
    assert_eq!(buffer.read_bytes(), [&[2; 16][..], &[3; 16]].concat());
    assert_eq!(memory.allocated_bytes(), 32);
    drop(buffer);
    drive(&mut controller, owner.shutdown()).unwrap();
}

#[test]
fn recovery_restart_reuses_budget_while_old_transport_bytes_remain_live() {
    use crate::replica_journal::{
        OwnedRecoveringJournal, OwnedRecoveryGenerations, OwnedRecoveryOpen, RecoveryStorage,
        ShardJournalConfig, ShardRecoveringJournal,
    };
    let (mut controller, io) = setup();
    let memory = payload_owner(64);
    let generations = |attempt| OwnedRecoveryGenerations {
        attempt: JournalGeneration(attempt),
        temporary: JournalGeneration(attempt + 1),
    };
    let (mut owner, _) = drive(
        &mut controller,
        OwnedRecoveringJournal::start(
            config("/replacement", 1, QuorumPolicy::Durable),
            io,
            generations(10),
            OwnedRecoveryOpen::FormatNew {
                segment_capacity: 32768,
            },
        ),
    )
    .unwrap();
    owner.bind_append_memory(&memory).unwrap();
    let mut buffer = owner.lease_append_buffer().unwrap();
    buffer.push_read_part(&[1; 32]).unwrap();
    let old = buffer.shared_bodies();
    drop(buffer);
    let mut recovering =
        ShardRecoveringJournal::from_owned(owner, ShardJournalConfig::default(), || 123).unwrap();
    for (attempt, full) in [(20, false), (30, true)] {
        (recovering, _) = drive(
            &mut controller,
            recovering.restart(full, generations(attempt)),
        )
        .unwrap();
        let mut buffer = recovering.lease_append_buffer().unwrap();
        assert!(
            matches!(buffer.push_read_part(&[2; 48]), Err(JournalError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
        assert_eq!(old.as_ref(), &[1; 32]);
        buffer.push_read_part(&[2; 32]).unwrap();
        assert_eq!(memory.allocated_bytes(), 64);
    }
    drive(&mut controller, recovering.shutdown()).unwrap();
    drop(recovering);
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 32);
    drop(old);
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 0);
}

#[test]
fn recovery_restart_preserves_reserved_allocation_source_and_unspent_credit() {
    use crate::replica_journal::{
        OwnedRecoveringJournal, OwnedRecoveryGenerations, OwnedRecoveryOpen, RecoveryStorage,
        ShardJournalConfig, ShardRecoveringJournal,
    };
    let (mut controller, io) = setup();
    let memory = payload_owner(64);
    let capacity = memory.capacity();
    let generations = |attempt| OwnedRecoveryGenerations {
        attempt: JournalGeneration(attempt),
        temporary: JournalGeneration(attempt + 1),
    };
    let (mut owner, _) = drive(
        &mut controller,
        OwnedRecoveringJournal::start(
            config("/replacement", 1, QuorumPolicy::Durable),
            io,
            generations(10),
            OwnedRecoveryOpen::FormatNew {
                segment_capacity: 32768,
            },
        ),
    )
    .unwrap();
    owner.bind_append_capacity(&capacity).unwrap();
    memory
        .reserve(
            &capacity,
            crate::memory::Quota {
                bytes: 32,
                buffers: 1,
            },
        )
        .unwrap();
    let mut buffer = owner.lease_append_buffer().unwrap();
    buffer.push_read_part(&[1; 32]).unwrap();
    let old = buffer.shared_bodies();
    drop(buffer);
    let mut recovering =
        ShardRecoveringJournal::from_owned(owner, ShardJournalConfig::default(), || 123).unwrap();
    for (attempt, full) in [(20, false), (30, true)] {
        (recovering, _) = drive(
            &mut controller,
            recovering.restart(full, generations(attempt)),
        )
        .unwrap();
        let mut buffer = recovering.lease_append_buffer().unwrap();
        // Free owner capacity is insufficient: this actor needs an explicit
        // reservation even after a new recovery generation is installed.
        assert!(matches!(buffer.push_read_part(&[2]),
            Err(JournalError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock));
        assert_eq!(capacity.remaining(), crate::memory::Quota::default());
        memory
            .reserve(
                &capacity,
                crate::memory::Quota {
                    bytes: 32,
                    buffers: 1,
                },
            )
            .unwrap();
        buffer.push_read_part(&[2; 32]).unwrap();
        assert_eq!(old.as_ref(), &[1; 32]);
        assert_eq!(memory.allocated_bytes(), 64);
    }
    drive(&mut controller, recovering.shutdown()).unwrap();
    drop((recovering, capacity));
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 32);
    drop(old);
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 0);
}

#[test]
fn journals_share_shard_payload_capacity_and_retained_replies_outlive_leases() {
    let (mut controller, io) = setup();
    let memory = payload_owner(64);
    let mut journals = Vec::new();
    for (id, path) in [(1, "/one"), (2, "/two")] {
        let (mut owner, _) = drive(
            &mut controller,
            OwnedJournal::format_new(
                config(path, id, QuorumPolicy::Durable),
                io.clone(),
                JournalGeneration(u128::from(id)),
                32768,
            ),
        )
        .unwrap();
        owner.bind_append_memory(&memory).unwrap();
        journals.push(owner);
    }
    let mut first = journals[0].lease_proposal_buffer().unwrap();
    let mut second = journals[1].lease_proposal_buffer().unwrap();
    // Each configured arena permits 8 KiB, but no body exists before intake.
    assert_eq!(memory.allocated_bytes(), 0);
    first.push(OperationKind::Barrier, &[1; 32]).unwrap();
    second.push(OperationKind::Barrier, &[2; 32]).unwrap();
    let network = first.0.shared_bodies();
    let alias = network.slice(0..1);
    drop((first, network));
    let mut reused = journals[0].lease_proposal_buffer().unwrap();
    assert!(
        matches!(reused.push(OperationKind::Barrier, &[3; 16]), Err(JournalError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    assert!(reused.is_empty());
    assert_eq!(memory.allocated_bytes(), 64);
    std::thread::spawn(move || drop(alias)).join().unwrap();
    reused.push(OperationKind::Barrier, &[3; 16]).unwrap();
    assert_eq!(reused.0.retained_bytes(), 32);
    let surviving = second.0.shared_bodies();
    drop((reused, second));
    for owner in journals {
        drive(&mut controller, owner.shutdown()).unwrap();
    }
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 32);
    assert_eq!(surviving.as_ref(), &[2; 32]);
    std::thread::spawn(move || drop(surviving)).join().unwrap();
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 0);
}

#[test]
fn memory_cannot_be_rebound_after_a_lease_escaped_even_when_returned() {
    let (mut controller, io) = setup();
    let (mut owner, _) = drive(
        &mut controller,
        OwnedJournal::format_new(
            config("/journal", 1, QuorumPolicy::Durable),
            io,
            JournalGeneration(1),
            32768,
        ),
    )
    .unwrap();
    drop(owner.lease_append_buffer().unwrap());
    assert!(matches!(
        owner.bind_append_memory(&payload_owner(64)),
        Err(JournalError::Configuration)
    ));
    drive(&mut controller, owner.shutdown()).unwrap();
}
