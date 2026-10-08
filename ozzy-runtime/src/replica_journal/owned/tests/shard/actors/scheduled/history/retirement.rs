use super::*;
use crate::replica_journal::owned::tests::{
    replay::persist,
    writeback::{confirm, replica},
};
use ozzy_proto::CheckpointId;
use ozzy_replication::wire::Control;

#[test]
fn election_bridges_retired_commit_with_a_remote_selected_source() {
    election_bridges_retired_commit(3);
}

#[test]
fn election_bridges_retired_commit_with_a_local_selected_source() {
    election_bridges_retired_commit(5);
}

#[expect(
    clippy::too_many_lines,
    reason = "one bounded, controlled retained-history election"
)]
fn election_bridges_retired_commit(installed_view: u64) {
    let (mut controller, io) = setup();
    let policy = QuorumPolicy::Durable;
    let mut primary = replica(&mut controller, io.clone(), 0, policy, 8192);
    let mut lagging = replica(&mut controller, io.clone(), 1, policy, 8192);
    let mut witness = replica(&mut controller, io.clone(), 2, policy, 8192);
    let mut accepted = Prefix::GENESIS;
    for number in 1..=4 {
        accepted = persist(&mut controller, &mut primary);
        persist(&mut controller, &mut witness);
        confirm(&mut primary, &witness, policy);
        if number <= 3 {
            persist(&mut controller, &mut lagging);
        }
        if number == 1 {
            lagging
                .driver
                .receive(
                    primary.config.identity.replica_node_id,
                    Control::Commit(ozzy_replication::Commit {
                        scope: primary.driver.scope(),
                        committed: accepted,
                    }),
                    Duration::ZERO,
                )
                .unwrap();
            let apply = lagging.driver.begin_validation().unwrap();
            lagging.journal.apply(apply).unwrap();
            lagging.driver.apply_through(apply.committed()).unwrap();
        }
        if number < 4 {
            let work = primary.journal.begin_roll(8).unwrap();
            let done = drive(&mut controller, work.publish());
            primary.journal.complete_roll(done).unwrap();
        }
    }
    let retired = drive(
        &mut controller,
        primary.journal.retire_confirmed_history(
            primary.driver.begin_validation().unwrap(),
            CheckpointId::from_bytes([94; 16]),
            ozzy_journal_segment::AsyncRetirementBudget {
                max_segments: 3,
                max_read_bytes: 3 * 32768,
            },
        ),
    )
    .unwrap();
    assert_eq!(retired.unreferenced_segment_ids, [1, 2, 3]);
    // Seed a later installed view on the advanced copy. The old voter retains
    // commit 1 and accepted 3; the chosen source retains only operation 4.
    drive(&mut controller, async {
        let stored = primary.journal.journal.ready_mut().unwrap();
        let mut manifest = stored.manifest().clone();
        manifest.parent_generation = manifest.generation;
        manifest.generation += 1;
        manifest.promised_view = installed_view;
        manifest.last_normal_view = installed_view;
        stored.install_metadata(manifest).await.unwrap();
    });
    let configs = [primary.config.clone(), lagging.config.clone()];
    drive(&mut controller, async {
        let stored = lagging.journal.journal.ready_mut().unwrap();
        let mut manifest = stored.manifest().clone();
        manifest.parent_generation = manifest.generation;
        manifest.generation += 1;
        manifest.accepted = crate::replica_journal::authority::position(
            lagging.driver.normal().unwrap().snapshot().accepted,
        );
        manifest.committed = crate::replica_journal::authority::position(
            lagging.driver.normal().unwrap().snapshot().committed,
        );
        stored.install_metadata(manifest).await.unwrap();
    });
    drive(&mut controller, primary.journal.shutdown()).unwrap();
    drive(&mut controller, lagging.journal.shutdown()).unwrap();
    drive(&mut controller, witness.journal.shutdown()).unwrap();
    let mut actors = Vec::new();
    for (index, config) in configs.into_iter().enumerate() {
        let (owner, startup) = drive(
            &mut controller,
            OwnedJournal::open(config, io.clone(), JournalGeneration(100 + index as u128)),
        )
        .unwrap();
        let recovered = startup.recovered().unwrap();
        if index == 1 {
            assert_eq!(recovered.log.committed.op.0, 1);
            assert_eq!(recovered.log.accepted.op.0, 3);
        }
        let journal = owner
            .into_shard_journal(
                ShardJournalConfig {
                    turn_steps: 4,
                    ..Default::default()
                },
                || 999,
            )
            .unwrap();
        actors.push(
            Scheduled::new(
                ReplicaActor::new_with_ids(
                    journal,
                    startup,
                    actor_config(),
                    crate::replica_actor::ActorIds::deterministic(
                        std::num::NonZeroU64::new(100 + index as u64).unwrap(),
                    ),
                )
                .unwrap(),
            )
            .unwrap(),
        );
    }
    let mut activated = false;
    for step in 0..20000 {
        let now = Duration::from_millis(step / 100);
        for (to, message) in collect(&mut actors, now, false) {
            if to < actors.len() {
                actors[to].receive(&message, now).unwrap();
            }
        }
        settle(&mut controller, &[]);
        if actors.iter().all(|actor| {
            let status = actor.status();
            status.scope.view > installed_view
                && status.application_ready
                && status
                    .normal
                    .is_some_and(|normal| normal.applied == accepted)
        }) {
            activated = true;
            break;
        }
    }
    assert!(
        activated,
        "retired ancestry stalled intact voters: {:?}",
        actors.iter().map(Scheduled::status).collect::<Vec<_>>()
    );
    for actor in actors {
        close(&mut controller, actor);
    }
}
