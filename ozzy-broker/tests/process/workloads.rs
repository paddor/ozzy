//! Varied bounded waves qualify the same oracle used by long churn runs.

use super::{Client, Fixture, cluster::Cluster};
use ozzy_config::Confirmation;
use std::time::Duration;

#[tokio::test(flavor = "current_thread")]
async fn cli_tcp_varied_payloads_and_live_reader_survive_leader_changes() {
    for policy in [Confirmation::DiskQuorum, Confirmation::ReplicatedPersisting] {
        let fixture = Cluster::retained(policy).pin_brokers();
        fixture.provision();
        let checked = fixture.checked(0);
        let mut brokers = fixture.start();
        let mut client = brokers
            .observe("open varied SDK", Client::open(&checked))
            .await;
        let mut reader = brokers
            .observe("open live reader", client.reader(false))
            .await;
        for wave in 0..18 {
            let positions = client.positions();
            let pending = brokers
                .observe("admit mixed records", client.queue_varied(wave))
                .await;
            if wave == 6 {
                let leader = client.leader(0);
                let index = (0..3)
                    .find(|index| {
                        checked.identity.brokers[&format!("broker-{index}")].as_bytes()
                            == leader.as_bytes()
                    })
                    .unwrap();
                brokers.stop(index, "KILL").await;
                brokers
                    .observe("confirm across leader kill", client.confirm(pending))
                    .await;
                brokers
                    .observe(
                        "reader follows new leader",
                        client.read(&mut reader, positions),
                    )
                    .await;
                if policy == Confirmation::ReplicatedPersisting {
                    brokers.restart_recovering(
                        &fixture,
                        index,
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
                } else {
                    brokers.restart(&fixture, index, 1);
                }
                brokers.ready(index).await;
            } else {
                brokers
                    .observe("confirm mixed records", client.confirm(pending))
                    .await;
                if wave % 6 == 2 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                brokers
                    .observe(
                        "verify live mixed payloads",
                        client.read(&mut reader, positions),
                    )
                    .await;
            }
            brokers
                .observe("verify history replay", client.replay())
                .await;
            client.discard_verified();
            if wave % 6 == 5 {
                client = brokers
                    .observe("resume varied producer", client.reopen_producer(wave == 11))
                    .await;
            }
        }
        brokers
            .observe("close live reader", reader.close())
            .await
            .unwrap();
        brokers.observe("close varied SDK", client.close()).await;
        brokers.shutdown().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cli_tcp_single_broker_varied_payloads_survive_sdk_resume() {
    let fixture = Fixture::new();
    fixture.provision_volumes();
    fixture.format();
    let mut broker = fixture.start(0);
    broker.ready().await;
    let mut client = broker
        .observe("open varied SDK", Client::open(&fixture.checked()))
        .await;
    let mut reader = broker
        .observe("open live reader", client.reader(false))
        .await;
    for wave in 0..12 {
        let positions = client.positions();
        let pending = broker
            .observe("admit mixed records", client.queue_varied(wave))
            .await;
        broker
            .observe("confirm mixed records", client.confirm(pending))
            .await;
        broker
            .observe("verify mixed records", client.read(&mut reader, positions))
            .await;
        client.discard_verified();
        if wave % 6 == 5 {
            client = broker
                .observe("resume varied producer", client.reopen_producer(wave == 11))
                .await;
        }
    }
    broker
        .observe("close live reader", reader.close())
        .await
        .unwrap();
    broker.observe("close varied SDK", client.close()).await;
    broker.signal("TERM");
    broker.exited(false).await;
}
