//! Explicit nonvoting replacement through the production shared frontend.

use super::*;
use ozzy_broker::{RecoveryIntent, RecoverySelection};

fn selected(intent: RecoveryIntent) -> RecoverySelection {
    RecoverySelection {
        topic: "orders".into(),
        partition: 0,
        intent,
    }
}

fn copy(checked: &CheckedConfig) -> CheckedConfig {
    CheckedConfig {
        deployment: checked.deployment.clone(),
        identity: checked.identity.clone(),
        plan: checked.plan.clone(),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn production_lost_partition_recovers_and_contributes_to_disk_quorum() {
    tokio::time::timeout(Duration::from_secs(35), replace(Confirmation::DiskQuorum))
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn production_lost_partition_recovers_and_contributes_to_replicated_persisting() {
    tokio::time::timeout(
        Duration::from_secs(35),
        replace(Confirmation::ReplicatedPersisting),
    )
    .await
    .unwrap();
}

async fn replace(policy: Confirmation) {
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let deployment = deployment(root.path(), DeploymentMode::Three, policy, 6);
    let restart = deployment
        .iter()
        .map(|(checked, local)| (copy(checked), local.clone()))
        .collect::<Vec<_>>();
    let sdk = links(&runtime, &deployment[0].0).await;
    let (mut writer, mut expected) = seed_history(&runtime, deployment, &sdk).await;
    let mut brokers = Vec::new();
    let (checked, local) = &restart[0];
    let lost = &checked
        .plan
        .partitions
        .iter()
        .find(|plan| plan.partition == 0)
        .unwrap()
        .directory;
    std::fs::remove_dir_all(lost).unwrap();
    assert!(
        Broker::start_trusted_with_context(copy(checked), local.clone(), runtime.context().clone())
            .await
            .is_err()
    );
    assert!(!lost.exists(), "ordinary startup invented missing history");
    let valid = selected(RecoveryIntent::Replace);
    let invalid = RecoverySelection {
        partition: 999,
        ..valid.clone()
    };
    assert!(
        Broker::start_recovering_trusted_with_context(
            copy(checked),
            local.clone(),
            &[valid.clone(), invalid],
            runtime.context().clone()
        )
        .await
        .is_err()
    );
    assert!(
        !lost.exists(),
        "partial validation mutated a selected store"
    );
    let recovered = Broker::start_recovering_trusted_with_context(
        copy(checked),
        local.clone(),
        &[valid],
        runtime.context().clone(),
    )
    .await
    .unwrap();
    assert_eq!(recovered.application_threads(), checked.plan.shards.len());
    assert_eq!(recovered.dispatcher_threads(), 1);
    assert_eq!(recovered.io_threads(), 1);
    brokers.push(recovered);
    brokers.push(
        Broker::start_trusted_with_context(
            copy(&restart[1].0),
            restart[1].1.clone(),
            runtime.context().clone(),
        )
        .await
        .unwrap(),
    );
    // Recovery needs both other brokers. This one remains absent while intact
    // neighboring partitions elect and confirm through the same shard/frontend.
    write_wave(&brokers, &mut writer, &mut expected, 5, true).await;
    brokers.push(
        Broker::start_trusted_with_context(
            copy(&restart[2].0),
            restart[2].1.clone(),
            runtime.context().clone(),
        )
        .await
        .unwrap(),
    );
    wait_for_publication(&brokers, checked, local, lost).await;
    let donor = brokers.remove(1);
    donor.shutdown().await.unwrap();
    drop(donor);
    // Only the rebuilt broker and broker 2 remain. Every new confirmation now
    // needs the rebuilt partition to finish recovery, elect, and retain history.
    write_wave(&brokers, &mut writer, &mut expected, 6, false).await;
    read_topic(&brokers, &sdk, &expected).await;
    assert_eq!(sdk.socket_count(), 5, "recovery added SDK sockets");
    live_many(&brokers, "close recovered topic writer", writer.close())
        .await
        .unwrap();
    sdk.shutdown().await.unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
}

async fn seed_history(
    runtime: &WriterRuntime,
    deployment: Vec<(CheckedConfig, BrokerIdentity)>,
    sdk: &BrokerLinks,
) -> (SharedTopicWriter, Vec<live::History>) {
    let brokers = start_brokers(runtime, deployment).await;
    let mut writer = live_many(
        &brokers,
        "open recovery test writer",
        SharedTopicWriter::open(
            sdk,
            "orders",
            SharedTopicWriterConfig::new(limits()),
            RetryPolicy::default(),
        ),
    )
    .await
    .unwrap();
    let mut expected = vec![Vec::new(); 6];
    write_wave(&brokers, &mut writer, &mut expected, 4, false).await;
    read_topic(&brokers, sdk, &expected).await;
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
    (writer, expected)
}

async fn write_wave(
    brokers: &[Broker],
    writer: &mut SharedTopicWriter,
    expected: &mut [live::History],
    wave: usize,
    exclude_recovering: bool,
) {
    let before = expected[0].len();
    let mut inputs = live::inputs(writer.metadata(), expected, wave, 4);
    if exclude_recovering {
        let key = inputs[0].2;
        inputs.retain(|input| input.2 != key);
        expected[0].truncate(before);
    }
    let mut pending = Vec::new();
    let mut offsets = expected
        .iter()
        .map(|history| history.len() - 4)
        .collect::<Vec<_>>();
    if exclude_recovering {
        offsets[0] = before;
    }
    for (id, body, key) in inputs {
        pending.push(
            writer
                .send(RecordInput::copy_from_slice(id, &body), Some(&key))
                .await
                .unwrap(),
        );
    }
    for pending in pending {
        let receipt = live_many(
            brokers,
            "confirm recovery cohort record",
            pending.confirmed(),
        )
        .await
        .unwrap();
        let partition = receipt.partition as usize;
        assert_eq!(receipt.record.offset, offsets[partition] as u64);
        offsets[partition] += 1;
    }
    assert_eq!(offsets, expected.iter().map(Vec::len).collect::<Vec<_>>());
}

async fn wait_for_publication(
    brokers: &[Broker],
    checked: &CheckedConfig,
    local: &BrokerIdentity,
    directory: &Path,
) {
    let plan = JournalPlan::from_trusted_deployment(checked, local).unwrap();
    let JournalConfig::Replicated(config) = &plan
        .partitions
        .iter()
        .find(|plan| plan.placement.partition == 0)
        .unwrap()
        .config
    else {
        panic!("replicated recovery became local");
    };
    let configuration = config.configuration.encode();
    live_many(brokers, "publish recovered configuration", async {
        loop {
            if std::fs::read(directory.join("CONFIGURATION"))
                .is_ok_and(|actual| actual == configuration)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn production_recovery_rejects_unknown_duplicate_and_local_selections_without_writes() {
    let root = tempfile::tempdir().unwrap();
    for (mode, policy) in [
        (DeploymentMode::Single, Confirmation::LocalDurable),
        (DeploymentMode::Three, Confirmation::DiskQuorum),
    ] {
        let (checked, local) = deployment(root.path(), mode, policy, 2).remove(0);
        let selection = selected(RecoveryIntent::Quarantine);
        if mode == DeploymentMode::Single {
            assert!(
                Broker::start_recovering_trusted(copy(&checked), local.clone(), &[selection])
                    .await
                    .is_err()
            );
        } else {
            assert!(
                Broker::start_recovering_trusted(
                    copy(&checked),
                    local.clone(),
                    &[selection.clone(), selection.clone()]
                )
                .await
                .is_err()
            );
            assert!(
                Broker::start_recovering_trusted(
                    copy(&checked),
                    local.clone(),
                    &[
                        selection,
                        RecoverySelection {
                            topic: "../orders".into(),
                            partition: 0,
                            intent: RecoveryIntent::Replace
                        }
                    ]
                )
                .await
                .is_err()
            );
        }
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn production_recovery_preflights_all_store_intents_before_any_mutation() {
    let root = tempfile::tempdir().unwrap();
    let (checked, local) = deployment(
        root.path(),
        DeploymentMode::Three,
        Confirmation::DiskQuorum,
        3,
    )
    .remove(0);
    for device in checked.deployment.deployment().brokers[&checked.plan.name]
        .devices
        .values()
    {
        std::fs::create_dir(&device.root).unwrap();
    }
    initialize_volumes(&checked, &local).unwrap();
    format_partition_journals(&checked, &local).await.unwrap();
    let first = &checked.plan.partitions[0].directory;
    let second = &checked.plan.partitions[1].directory;
    let configuration = std::fs::read(second.join("CONFIGURATION")).unwrap();
    std::fs::remove_dir_all(first).unwrap();
    for intent in [
        RecoveryIntent::Replace,
        RecoveryIntent::Resume,
        RecoveryIntent::ResumeFull,
    ] {
        let invalid = RecoverySelection {
            partition: 1,
            ..selected(intent)
        };
        assert!(
            Broker::start_recovering_trusted(
                copy(&checked),
                local.clone(),
                &[selected(RecoveryIntent::Replace), invalid]
            )
            .await
            .is_err()
        );
        assert!(
            !first.exists(),
            "invalid later intent formatted first partition"
        );
        assert_eq!(
            std::fs::read(second.join("CONFIGURATION")).unwrap(),
            configuration
        );
    }
    // A valid quarantine must also wait for the entire selection's preflight.
    let quarantine = RecoverySelection {
        partition: 1,
        ..selected(RecoveryIntent::Quarantine)
    };
    let occupied = RecoverySelection {
        partition: 2,
        ..selected(RecoveryIntent::Replace)
    };
    assert!(
        Broker::start_recovering_trusted(copy(&checked), local.clone(), &[quarantine, occupied])
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read(second.join("CONFIGURATION")).unwrap(),
        configuration
    );
    assert!(!first.exists());
}
