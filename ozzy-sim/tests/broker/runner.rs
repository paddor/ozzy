use super::*;
use ozzy_sim::soak::{self, Action, Config, Event, Resources, Time};

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_manual_runner_replays_churn_under_small_owner_limits() {
    replay(false).await;
}

mod stress {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn inproc_memory_manual_runner_replays_retention_and_churn_under_small_owner_limits() {
        replay(true).await;
    }
}

async fn replay(retention: bool) {
    let actions = actions(retention);
    tokio::time::timeout(
        Duration::from_secs(if retention { 900 } else { 120 }),
        async {
            for policy in [
                Confirmation::LocalDurable,
                Confirmation::DiskQuorum,
                Confirmation::ReplicatedPersisting,
            ] {
                let root = tempfile::tempdir().unwrap();
                let prefix = root.path().join("prefix.jsonl");
                let schedule = actions
                    .iter()
                    .enumerate()
                    .map(|(wave, &action)| {
                        serde_json::to_string(&Event {
                            wave,
                            pattern: wave % 6,
                            action,
                            time_millis: 0,
                        })
                        .unwrap()
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                std::fs::write(&prefix, format!("{schedule}\n")).unwrap();
                let result = soak::run(&Config {
                    policy,
                    seed: 1,
                    duration: Duration::from_secs(if retention { 300 } else { 90 }),
                    waves: actions.len(),
                    interval: Duration::ZERO,
                    progress_timeout: Duration::from_secs(if retention { 120 } else { 20 }),
                    artifacts: root.path().join("run"),
                    replay: Some(prefix),
                    time: Time {
                        duration: Some(Duration::from_secs(if retention { 3000 } else { 600 })),
                        ..Default::default()
                    },
                    resources: Resources {
                        partitions: 2,
                        resident_bytes: 16 * 1024 * 1024,
                        physical_jobs: 16,
                        physical_bytes: 4 * 1024 * 1024,
                        ..Default::default()
                    },
                    actions: soak::default_actions(),
                })
                .await;
                let report = result.unwrap_or_else(|error| {
                    panic!("{error}; artifacts: {}", root.keep().display())
                });
                assert_eq!(report.waves, actions.len());
                assert!(report.complete);
                assert_eq!(report.shared_producers, 1);
                assert_eq!(report.producers, 2);
                assert_eq!(report.resumes, 1);
                assert_eq!(report.takeovers, 1);
                assert_eq!(report.reconnects, 1);
                assert_eq!(report.completion_holds, 1);
                assert_eq!(
                    report.restarts,
                    3 + 2 * usize::from(policy != Confirmation::LocalDurable)
                );
                assert_eq!(report.slow_consumers, 1);
                assert_eq!(report.retention_gaps, usize::from(retention));
                assert_eq!(report.process_crashes, 1);
                assert_eq!(report.power_losses, 1);
                assert_eq!(
                    report.quorum_losses,
                    usize::from(policy != Confirmation::LocalDurable)
                );
                assert_eq!(
                    report.canceled_observations,
                    if policy == Confirmation::LocalDurable {
                        0
                    } else {
                        8
                    }
                );
                assert_eq!(
                    report.torn_writes,
                    usize::from(policy != Confirmation::LocalDurable)
                );
                assert!(report.records > 0 && report.physical_events > 0 && report.time_millis > 0);
            }
        },
    )
    .await
    .expect("bounded manual runner lost progress");
}

fn actions(retention: bool) -> Vec<Action> {
    [
        Action::Traffic,
        Action::SharedProducers,
        Action::Takeover,
        Action::Resume,
        Action::Reconnect,
        Action::Completions,
        Action::Restart,
        Action::Consumer,
        Action::TornWrite,
        Action::SlowConsumer,
        Action::QuorumLoss,
        Action::RetentionLag,
        Action::ProcessCrash,
        Action::PowerLoss,
    ]
    .into_iter()
    .filter(|action| retention || !matches!(action, Action::RetentionLag))
    .collect::<Vec<_>>()
}
