//! TCP framing, disconnect/retry, process death, and CLI shutdown use production owners.

mod client;
mod cluster;
mod fixture;
mod recovery;
mod soak;
mod workloads;

pub(crate) use client::Client;
use fixture::Fixture;
use std::{pin::pin, time::Duration};

#[test]
fn cli_serve_requires_trust_and_never_formats_missing_history() {
    let fixture = Fixture::new();
    let missing_trust = fixture.serve(false).output().unwrap();
    assert!(!missing_trust.status.success());
    assert!(String::from_utf8_lossy(&missing_trust.stderr).contains("--trusted-transport"));
    let missing_identity = fixture.serve(true).output().unwrap();
    assert!(!missing_identity.status.success());
    fixture.assert_no_history();
    fixture.provision_volumes();
    let missing_history = fixture.serve(true).output().unwrap();
    assert!(!missing_history.status.success());
    fixture.assert_no_history();
    // Failed normal startup must release every opened worker/handle so explicit
    // formatting and ordinary serving can follow on the exact same paths.
    fixture.format();
}

#[test]
fn cli_recover_requires_selection_trust_and_valid_partition_text() {
    let fixture = Fixture::new();
    for (selections, trusted, expected) in [
        (
            ["--replace", "orders/0"].as_slice(),
            false,
            "--trusted-transport",
        ),
        ([].as_slice(), true, "--replace"),
        (
            ["--replace", "orders"].as_slice(),
            true,
            "expected TOPIC/PARTITION",
        ),
        (
            ["--quarantine", "/0"].as_slice(),
            true,
            "expected a topic name",
        ),
        (
            ["--resume", "orders/-1"].as_slice(),
            true,
            "expected a partition number",
        ),
        (
            ["--resume-full", "orders/4294967296"].as_slice(),
            true,
            "expected a partition number",
        ),
    ] {
        let output = fixture.recover(selections, trusted).output().unwrap();
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "{selections:?}: {output:?}");
        assert!(error.contains(expected), "{selections:?}: {error}");
        fixture.assert_no_history();
    }
    let output = fixture
        .serve(true)
        .args(["--replace", "orders/0"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unexpected argument '--replace'"));
    fixture.assert_no_history();
}

#[test]
fn cli_recover_refuses_local_stores_without_creating_history() {
    let fixture = Fixture::new();
    fixture.provision_volumes();
    for flag in ["--replace", "--quarantine", "--resume", "--resume-full"] {
        let output = fixture.recover(&[flag, "orders/0"], true).output().unwrap();
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "{flag}: {output:?}");
        assert!(
            error.contains("single-broker partitions cannot recover"),
            "{flag}: {error}"
        );
        fixture.assert_no_history();
    }
    fixture.format();
}

#[tokio::test(flavor = "current_thread")]
async fn cli_tcp_crash_restart_and_signal_drain_preserve_confirmed_records() {
    tokio::time::timeout(Duration::from_secs(60), scenario(Fixture::new()))
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cli_tcp_three_brokers_restart_and_confirm_disk_quorum() {
    tokio::time::timeout(
        Duration::from_secs(60),
        cluster::orderly_restart(ozzy_config::Confirmation::DiskQuorum),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cli_tcp_three_brokers_restart_and_confirm_replicated_persisting() {
    tokio::time::timeout(
        Duration::from_secs(60),
        cluster::orderly_restart(ozzy_config::Confirmation::ReplicatedPersisting),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cli_tcp_saved_producer_resumes_and_takes_over_after_broker_kills() {
    for policy in [
        ozzy_config::Confirmation::DiskQuorum,
        ozzy_config::Confirmation::ReplicatedPersisting,
    ] {
        tokio::time::timeout(
            Duration::from_secs(60),
            saved_identity_after_crashes(policy),
        )
        .await
        .unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cli_tcp_retained_history_confirms_with_two_copies_after_active_leader_kill() {
    for policy in [
        ozzy_config::Confirmation::DiskQuorum,
        ozzy_config::Confirmation::ReplicatedPersisting,
    ] {
        let fixture = cluster::Cluster::retained(policy).pin_brokers();
        fixture.provision();
        let checked = fixture.checked(0);
        let mut brokers = fixture.start();
        let mut client = brokers
            .observe("open retained producer", Client::open(&checked))
            .await;
        for wave in 0..12 {
            let pending = client.queue_large(wave, 256).await;
            brokers
                .observe("confirm retention load", client.confirm(pending))
                .await;
            brokers
                .observe("verify retention load", client.replay())
                .await;
            client.discard_verified();
            if wave % 4 == 3 {
                client = brokers
                    .observe("resume retention producer", client.reopen_producer(false))
                    .await;
            }
        }
        for wave in 100..356 {
            let pending = client.queue(wave).await;
            brokers
                .observe("confirm reader churn", client.confirm(pending))
                .await;
            brokers
                .observe("verify reader churn", client.replay())
                .await;
            client.discard_verified();
            if wave % 64 == 0 {
                client = brokers
                    .observe("resume churn producer", client.reopen_producer(false))
                    .await;
            }
        }
        let floor = brokers
            .observe("verify native retirement", client.retained_floor())
            .await;
        assert!(floor.get() > 0, "{policy:?}: no sealed history retired");
        let leader = client.leader(0);
        let index = (0..3)
            .find(|index| {
                ozzy_proto::NodeId::from_bytes(
                    *checked.identity.brokers[&format!("broker-{index}")].as_bytes(),
                ) == leader
            })
            .unwrap();
        brokers.stop(index, "KILL").await;
        let pending = client.queue(999).await;
        brokers
            .observe(
                "confirm with retained leader absent",
                client.confirm(pending),
            )
            .await;
        brokers
            .observe("verify after retained leader kill", client.replay())
            .await;
        client.discard_verified();
        client = brokers
            .observe(
                "takeover after retained leader kill",
                client.reopen_producer(true),
            )
            .await;
        let pending = client.queue(1000).await;
        brokers
            .observe("confirm takeover after churn", client.confirm(pending))
            .await;
        brokers
            .observe("verify takeover after churn", client.replay())
            .await;
        brokers
            .observe("close retained producer", client.close())
            .await;
        brokers.shutdown().await;
    }
}

async fn saved_identity_after_crashes(policy: ozzy_config::Confirmation) {
    let fixture = cluster::Cluster::new(policy);
    fixture.provision();
    let checked = fixture.checked(0);
    let mut brokers = fixture.start();
    let mut client = brokers
        .observe("open saved producer", Client::open(&checked))
        .await;
    let pending = client.queue(0).await;
    brokers
        .observe("confirm saved producer", client.confirm(pending))
        .await;
    brokers.stop(0, "KILL").await;
    client = brokers
        .observe("resume after broker kill", client.reopen_producer(false))
        .await;
    let pending = client.queue(1).await;
    brokers
        .observe("confirm resumed producer", client.confirm(pending))
        .await;
    if policy == ozzy_config::Confirmation::ReplicatedPersisting {
        brokers.restart_recovering(
            &fixture,
            0,
            1,
            &[
                "--quarantine",
                "orders/0",
                "--quarantine",
                "orders/1",
                "--quarantine",
                "orders/2",
                "--quarantine",
                "orders/3",
            ],
        );
        brokers
            .observe("recover killed memory voter", async {
                loop {
                    if (0..4).all(|number| {
                        std::fs::read(fixture.partition(0, number).join("CONFIGURATION")).is_ok_and(
                            |bytes| ozzy_replication::ConfigurationRecord::decode(&bytes).is_ok(),
                        )
                    }) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await;
    } else {
        brokers.restart(&fixture, 0, 1);
    }
    let pending = client.queue(2).await;
    brokers
        .observe("confirm after recovery handoff", client.confirm(pending))
        .await;
    // This TCP layer has real timers and independent process schedulers. Leave
    // the restarted voter time to activate before removing another normal copy.
    tokio::time::sleep(Duration::from_secs(1)).await;
    brokers.stop(1, "KILL").await;
    client = brokers
        .observe("takeover after broker kill", client.reopen_producer(true))
        .await;
    let pending = client.queue(3).await;
    brokers
        .observe("confirm takeover", client.confirm(pending))
        .await;
    brokers
        .observe("verify retained producer payloads", client.replay())
        .await;
    brokers
        .observe("close saved producer", client.close())
        .await;
    brokers.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires Podman and a local OZZY_BROKER_IMAGE built from the current binary"]
async fn container_single_broker_initializes_restarts_and_drains() {
    let image = std::env::var("OZZY_BROKER_IMAGE").expect("set OZZY_BROKER_IMAGE");
    tokio::time::timeout(Duration::from_secs(60), scenario(Fixture::container(image)))
        .await
        .unwrap();
}

async fn scenario(fixture: Fixture) {
    fixture.provision_volumes();
    fixture.format();
    let checked = fixture.checked();
    let broker_id = ozzy_proto::NodeId::from_bytes(*checked.identity.brokers["laptop"].as_bytes());
    let mut broker = fixture.start(0);
    let mut client = broker.observe("open SDK", Client::open(&checked)).await;
    assert_eq!(client.links.socket_count(), 3);
    let pending = client.queue(0).await;
    broker
        .observe("confirm before crash", client.confirm(pending))
        .await;
    broker
        .observe("read initial history", client.replay())
        .await;
    let mut reader = broker
        .observe("open reader before crash", client.reader(true))
        .await;
    let checkpoint = client.positions();
    {
        let mut next = pin!(reader.next());
        assert!(futures::poll!(next.as_mut()).is_pending());
    }
    let old_session = client.links.session(broker_id).unwrap();
    broker.signal("KILL");
    broker.exited(true).await;
    let pending = client.queue(1).await;
    {
        let mut canceled = pin!(pending[0].0.confirmed());
        assert!(futures::poll!(canceled.as_mut()).is_pending());
    }
    broker = fixture.start(1);
    broker
        .observe("confirm queued retry after crash", client.confirm(pending))
        .await;
    assert_ne!(client.links.session(broker_id), Some(old_session));
    assert_eq!(client.links.socket_count(), 3);
    broker
        .observe(
            "repair reader after process death",
            client.read(&mut reader, checkpoint),
        )
        .await;
    broker
        .observe("close repaired reader", reader.close())
        .await
        .unwrap();
    // These individually admitted records may or may not reach the broker
    // before SIGTERM. Retry must resolve their exact identity after restart.
    let pending = client.queue(2).await;
    broker.signal("TERM");
    broker.exited(false).await;
    broker = fixture.start(2);
    broker
        .observe("confirm after graceful restart", client.confirm(pending))
        .await;
    broker
        .observe("read exact complete history", client.replay())
        .await;
    broker.observe("close SDK", client.close()).await;
    broker.signal("INT");
    broker.exited(false).await;
}
