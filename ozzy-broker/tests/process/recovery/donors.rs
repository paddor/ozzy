//! A donor's new process session cannot stall healthy neighboring partitions.

use super::{Client, Cluster, Processes};
use ozzy_config::Confirmation;
use ozzy_proto::{LinkSessionId, NodeId};
use std::{fs, time::Duration};

#[tokio::test(flavor = "current_thread")]
async fn cli_changed_donor_sessions_preserve_neighbor_progress_disk_quorum() {
    tokio::time::timeout(Duration::from_secs(60), scenario(Confirmation::DiskQuorum))
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cli_changed_donor_sessions_preserve_neighbor_progress_replicated_persisting() {
    tokio::time::timeout(
        Duration::from_secs(60),
        scenario(Confirmation::ReplicatedPersisting),
    )
    .await
    .unwrap();
}

async fn scenario(policy: Confirmation) {
    let cluster = Cluster::new(policy);
    cluster.provision();
    let checked = cluster.checked(0);
    let donor = NodeId::from_bytes(*checked.identity.brokers["broker-1"].as_bytes());
    let mut processes = cluster.start();
    let mut client = processes
        .observe("open donor-restart SDK", Client::open(&checked))
        .await;
    let pending = client.queue(0).await;
    processes
        .observe("confirm initial donor history", client.confirm(pending))
        .await;
    processes
        .observe("verify initial donor history", client.replay())
        .await;
    let previous = client.links.session(donor).unwrap();
    processes.shutdown().await;

    let replaced = cluster.partition(0, 0);
    let configuration = fs::read(replaced.join("CONFIGURATION")).unwrap();
    let neighbors: [Vec<u8>; 3] = std::array::from_fn(|number| {
        fs::read(
            cluster
                .partition(0, number as u32 + 1)
                .join("CONFIGURATION"),
        )
        .unwrap()
    });
    fs::remove_dir_all(&replaced).unwrap();
    processes.restart_recovering(&cluster, 0, 1, &["--replace", "orders/0"]);
    processes.ready(0).await;
    let marker = fs::read(replaced.join("CONFIGURATION")).unwrap();
    assert!(marker.starts_with(b"OZYRECOV"));
    processes.restart(&cluster, 1, 1);
    processes.ready(1).await;
    let first = processes
        .observe(
            "first donor session",
            changed_session(&client, donor, previous),
        )
        .await;
    confirm_neighbors(&mut processes, &mut client, 1).await;
    assert_eq!(fs::read(replaced.join("CONFIGURATION")).unwrap(), marker);

    // Graceful restart preserves this donor's proven voting history under both
    // policies. Recovery still cannot publish until the second donor returns.
    processes.stop(1, "TERM").await;
    processes.restart(&cluster, 1, 2);
    processes.ready(1).await;
    processes
        .observe(
            "replacement donor session",
            changed_session(&client, donor, first),
        )
        .await;
    confirm_neighbors(&mut processes, &mut client, 2).await;
    assert_eq!(fs::read(replaced.join("CONFIGURATION")).unwrap(), marker);
    for (number, before) in neighbors.iter().enumerate() {
        assert_eq!(
            fs::read(
                cluster
                    .partition(0, number as u32 + 1)
                    .join("CONFIGURATION")
            )
            .unwrap(),
            *before
        );
    }

    processes.restart(&cluster, 2, 1);
    processes.ready(2).await;
    processes
        .observe("publish with current donor sessions", async {
            loop {
                if fs::read(replaced.join("CONFIGURATION"))
                    .is_ok_and(|bytes| bytes == configuration)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
    // With donor 1 absent, every new confirmation needs brokers 0 and 2. Merely
    // publishing recovered files cannot satisfy this independent SDK oracle.
    processes.stop(1, "TERM").await;
    let pending = client.queue(3).await;
    processes
        .observe("confirmation needs rebuilt broker", client.confirm(pending))
        .await;
    processes
        .observe("verify recovered and neighboring history", client.replay())
        .await;
    assert_eq!(client.links.socket_count(), 5);
    processes
        .observe("close donor-restart SDK", client.close())
        .await;
    processes.shutdown().await;
}

async fn confirm_neighbors(processes: &mut Processes, client: &mut Client, wave: usize) {
    let pending = client.queue_except(wave, 0).await;
    processes
        .observe(
            &format!("neighbors progress without second donor, wave {wave}"),
            client.confirm(pending),
        )
        .await;
}

async fn changed_session(client: &Client, donor: NodeId, previous: LinkSessionId) -> LinkSessionId {
    loop {
        if let Some(current) = client.links.session(donor)
            && current != previous
        {
            return current;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
