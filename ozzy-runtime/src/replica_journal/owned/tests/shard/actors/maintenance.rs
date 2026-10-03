use super::*;
use ozzy_journal_segment::MaintenanceBudget;

#[test]
fn maintenance_serves_oldest_deadline_with_foreground_turns_between_jobs() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let mut actors: Vec<_> = (0..3)
            .map(|broker| {
                let actor = unstarted_actor(&mut controller, &io, 0, policy, broker, 64);
                let actor = if broker == 0 {
                    actor
                        .with_metadata_cleanup(
                            Duration::from_millis(1),
                            MaintenanceBudget::default(),
                        )
                        .unwrap()
                        .with_orphan_cleanup(Duration::from_millis(2), MaintenanceBudget::default())
                        .unwrap()
                        .with_storage_validation(Duration::from_millis(3))
                        .unwrap()
                } else {
                    actor
                };
                ControlledReplica::new(actor)
            })
            .collect();
        for _ in 0..1000 {
            round(&mut actors, Duration::ZERO);
            for (id, _) in controller.jobs() {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
            if actors.iter().all(|actor| actor.status().application_ready) {
                break;
            }
        }
        assert!(actors.iter().all(|actor| actor.status().application_ready));
        let now = Duration::from_millis(3);
        let leader = &mut actors[0];
        for expected in [(1, 0, 0), (1, 1, 0), (1, 1, 1)] {
            leader.advance(now).unwrap();
            assert!(drive(&mut controller, leader.complete_disk(now)).unwrap());
            let status = leader.status();
            assert_eq!(
                (
                    status.maintenance.metadata_steps,
                    status.maintenance.orphan_steps,
                    status.maintenance.validation_steps,
                ),
                expected,
            );
            // Every maintenance completion reserves a foreground scheduling turn.
            leader.advance(now).unwrap();
            assert!(!drive(&mut controller, leader.complete_disk(now)).unwrap());
            assert_eq!(leader.status().maintenance, status.maintenance);
        }
        for actor in actors {
            close(&mut controller, actor);
        }
    }
}
