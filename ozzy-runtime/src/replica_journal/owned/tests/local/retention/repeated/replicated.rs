use super::*;
use crate::replica_journal::owned::tests::{
    replay::persist,
    writeback::{Replica, confirm, replica},
};

#[test]
fn repeated_retirement_preserves_both_quorum_policies_and_the_retained_chain() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let mut primary = replica(&mut controller, io.clone(), 0, policy, 8192);
        let mut backup = replica(&mut controller, io, 2, policy, 8192);
        let mut prefixes = Vec::new();
        for at in 0..3 {
            prefixes.push(persist(&mut controller, &mut primary));
            persist(&mut controller, &mut backup);
            confirm(&mut primary, &backup, policy);
            if at != 2 {
                roll(&mut controller, &mut primary.journal);
            }
        }
        let ticket = primary.driver.begin_validation().unwrap();
        let before = primary.driver.normal().unwrap().snapshot();
        let owner = primary.journal.append_memory.clone().unwrap();
        owner.trim_cache();
        let occupied = owner
            .try_charge(1024 * 1024 - owner.allocated_bytes())
            .unwrap();
        assert!(matches!(
            drive(
                &mut controller,
                primary.journal.retire_confirmed_history(
                    ticket,
                    CheckpointId::from_bytes([91; 16]),
                    AsyncRetirementBudget {
                        max_segments: 1,
                        max_read_bytes: 32768
                    },
                )
            ),
            Err(JournalError::AppendCapacity)
        ));
        assert!(!primary.journal.is_faulted());
        assert!(
            primary
                .journal
                .journal
                .ready()
                .unwrap()
                .manifest()
                .checkpoint
                .is_none()
        );
        assert_eq!(primary.driver.normal().unwrap().snapshot(), before);
        drop(occupied);

        let selected = retire(&mut controller, &mut primary.journal, ticket, 92, &[1]);
        assert_eq!(
            retire(&mut controller, &mut primary.journal, ticket, 93, &[2]),
            selected
        );
        assert_eq!(
            retire(&mut controller, &mut primary.journal, ticket, 94, &[]),
            selected
        );
        assert_eq!(primary.driver.normal().unwrap().snapshot(), before);
        assert!(!primary.journal.is_faulted());
        let request = ozzy_replication::wire::FetchOps {
            scope: ticket.scope(),
            request_id: ozzy_proto::RequestId::from_bytes([95; 16]),
            source: ozzy_replication::LogSource {
                voter: primary.config.identity.replica_node_id,
                generation: ticket.generation(),
                accepted: ticket.accepted(),
            },
            predecessor: prefixes[1],
            max_operations: 4,
            max_body_bytes: 8192,
        };
        let buffer = primary.journal.lease_append_buffer().unwrap();
        let fetched = drive(
            &mut controller,
            primary.journal.fetch_history(request, buffer),
        )
        .unwrap();
        assert_eq!(fetched.end(), prefixes[2]);
        let operations = fetched.buffer().operations().collect::<Vec<_>>();
        assert_eq!(operations.len(), 1);
        assert_eq!(operations[0].previous_digest, prefixes[1].digest);
        assert_eq!(operations[0].body, &[3; 16]);
        drop(fetched);
        drive(&mut controller, primary.journal.shutdown()).unwrap();
        drive(&mut controller, backup.journal.shutdown()).unwrap();
    }
}

#[test]
fn a_promise_abandons_an_unfinished_retirement_without_fencing() {
    let policy = QuorumPolicy::Durable;
    let (mut controller, io) = setup();
    let mut primary = replica(&mut controller, io.clone(), 0, policy, 8192);
    let mut backup = replica(&mut controller, io, 2, policy, 8192);
    for at in 0..2 {
        persist(&mut controller, &mut primary);
        persist(&mut controller, &mut backup);
        confirm(&mut primary, &backup, policy);
        if at == 0 {
            roll(&mut controller, &mut primary.journal);
        }
    }
    let ticket = primary.driver.begin_validation().unwrap();
    drive(
        &mut controller,
        primary.journal.begin_retirement(
            ticket,
            CheckpointId::from_bytes([91; 16]),
            AsyncRetirementBudget {
                max_segments: 1,
                max_read_bytes: 32768,
            },
        ),
    )
    .unwrap();
    let Replica {
        mut journal,
        driver,
        ..
    } = primary;
    let work = journal.take_retirement_work().unwrap();
    let done = drive(&mut controller, Box::pin(work.run()));
    // A view change replaces the owner's images before the work completes.
    let ozzy_replication::driver::Role::Normal(normal) = driver.into_role() else {
        panic!("normal primary");
    };
    let promise = normal.into_view_change(1).unwrap().begin_promise().unwrap();
    drive(&mut controller, journal.persist_promise(promise)).unwrap();
    assert!(!journal.retiring());
    journal.complete_retirement(done);
    assert!(!journal.retiring());
    assert!(
        drive(&mut controller, journal.finish_retirement())
            .unwrap()
            .is_none()
    );
    assert!(!journal.is_faulted());
    let manifest = journal.journal.ready().unwrap().manifest();
    assert!(manifest.checkpoint.is_none());
    assert_eq!(manifest.segments[0].segment_id, 1);
    drive(&mut controller, journal.shutdown()).unwrap();
}
