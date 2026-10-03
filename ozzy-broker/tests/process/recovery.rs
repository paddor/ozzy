//! CLI selection and store-state checks must precede every selected mutation.

mod donors;

use super::{
    Client,
    cluster::{Cluster, Processes},
};
use ozzy_config::Confirmation;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

fn refused(mut command: Command) -> String {
    let output = command.output().unwrap();
    assert!(!output.status.success(), "{command:?}: {output:?}");
    String::from_utf8(output.stderr).unwrap()
}

fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn collect(root: &Path, next: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(next).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                collect(root, &path, files);
            } else {
                files.insert(
                    path.strip_prefix(root).unwrap().to_owned(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut files = BTreeMap::new();
    collect(root, root, &mut files);
    files
}

#[test]
fn cli_recovery_preflights_every_replicated_selection_before_mutation() {
    for policy in [Confirmation::DiskQuorum, Confirmation::ReplicatedPersisting] {
        let cluster = Cluster::new(policy);
        cluster.provision();
        let lost = cluster.partition(0, 0);
        let intact = cluster.partition(0, 1);
        let before = snapshot(&intact);
        fs::remove_dir_all(&lost).unwrap();
        for selections in [
            ["--replace", "orders/0", "--replace", "orders/999"],
            ["--replace", "orders/0", "--quarantine", "orders/0"],
            ["--replace", "orders/0", "--replace", "orders/1"],
            ["--replace", "orders/0", "--resume", "orders/1"],
            ["--replace", "orders/0", "--resume-full", "orders/1"],
            ["--quarantine", "orders/1", "--replace", "orders/2"],
        ] {
            let error = refused(cluster.recover(0, &selections));
            assert!(!error.is_empty(), "{policy:?}: {selections:?}");
            assert!(
                !lost.exists(),
                "later invalid selection created a replacement"
            );
            assert_eq!(
                snapshot(&intact),
                before,
                "later invalid selection mutated intact store"
            );
        }
        refused(cluster.serve(0));
        assert!(!lost.exists(), "ordinary serving created missing history");
        assert_eq!(snapshot(&intact), before);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cli_replacement_and_quarantine_start_nonvoting_without_donors() {
    tokio::time::timeout(Duration::from_secs(60), async {
        for policy in [Confirmation::DiskQuorum, Confirmation::ReplicatedPersisting] {
            let cluster = Cluster::new(policy);
            cluster.provision();
            let replaced = cluster.partition(0, 0);
            let quarantined = cluster.partition(0, 1);
            let configuration = fs::read(quarantined.join("CONFIGURATION")).unwrap();
            let segments = snapshot(&quarantined.join("segments"));
            assert!(!segments.is_empty());
            fs::remove_dir_all(&replaced).unwrap();
            let mut processes = Processes::default();
            processes.restart_recovering(
                &cluster,
                0,
                0,
                &["--replace", "orders/0", "--quarantine", "orders/1"],
            );
            processes.ready(0).await;
            let replacement_marker = fs::read(replaced.join("CONFIGURATION")).unwrap();
            let quarantine_marker = fs::read(quarantined.join("CONFIGURATION")).unwrap();
            assert!(replacement_marker.starts_with(b"OZYRECOV"));
            assert!(quarantine_marker.starts_with(b"OZYRECOV"));
            assert_ne!(quarantine_marker, configuration);
            for (path, bytes) in segments {
                assert_eq!(
                    fs::read(quarantined.join("segments").join(path)).unwrap(),
                    bytes
                );
            }
            processes.stop(0, "TERM").await;
            let before = [snapshot(&replaced), snapshot(&quarantined)];
            refused(cluster.serve(0));
            assert_eq!([snapshot(&replaced), snapshot(&quarantined)], before);
            assert_eq!(
                fs::read(replaced.join("CONFIGURATION")).unwrap(),
                replacement_marker
            );
            assert_eq!(
                fs::read(quarantined.join("CONFIGURATION")).unwrap(),
                quarantine_marker
            );
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cli_interrupted_transfer_resumes_exact_history_with_disk_quorum() {
    tokio::time::timeout(
        Duration::from_secs(60),
        interrupted(Confirmation::DiskQuorum),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cli_interrupted_transfer_resumes_exact_history_with_replicated_persisting() {
    tokio::time::timeout(
        Duration::from_secs(60),
        interrupted(Confirmation::ReplicatedPersisting),
    )
    .await
    .unwrap();
}

async fn interrupted(policy: Confirmation) {
    let cluster = Cluster::new(policy);
    cluster.provision();
    let mut processes = cluster.start();
    let (client, probe) = seed(&cluster, &mut processes).await;

    let replaced = cluster.partition(0, 0);
    let quarantined = cluster.partition(0, 1);
    let configurations: [Vec<u8>; 4] = std::array::from_fn(|number| {
        fs::read(cluster.partition(0, number as u32).join("CONFIGURATION")).unwrap()
    });
    let old_segments = snapshot(&quarantined.join("segments"));
    fs::remove_dir_all(&replaced).unwrap();
    processes.restart_recovering(
        &cluster,
        0,
        1,
        &["--replace", "orders/0", "--quarantine", "orders/1"],
    );
    processes.ready(0).await;
    for (path, bytes) in old_segments {
        assert_eq!(
            fs::read(quarantined.join("segments").join(path)).unwrap(),
            bytes
        );
    }
    let marker = fs::read(replaced.join("CONFIGURATION")).unwrap();
    assert!(marker.starts_with(b"OZYRECOV"));
    processes.stop(0, "KILL").await;
    refused(cluster.serve(0));
    let selections = after_crash(&cluster, policy);
    assert_eq!(
        &selections[..4],
        &["--resume", "orders/0", "--resume-full", "orders/1"]
    );
    processes.restart_recovering(&cluster, 0, 2, &selections);
    processes.ready(0).await;
    assert_eq!(fs::read(replaced.join("CONFIGURATION")).unwrap(), marker);
    assert!(
        fs::read(quarantined.join("CONFIGURATION"))
            .unwrap()
            .starts_with(b"OZYRECOV")
    );
    processes.restart(&cluster, 1, 1);
    processes.restart(&cluster, 2, 1);
    receive_and_interrupt(&mut processes, &replaced, &marker, &probe).await;
    refused(cluster.serve(0));
    assert_eq!(fs::read(replaced.join("CONFIGURATION")).unwrap(), marker);

    let mut damaged = marker.clone();
    damaged[16] ^= 1;
    fs::write(replaced.join("CONFIGURATION"), damaged).unwrap();
    let before = [snapshot(&replaced), snapshot(&quarantined)];
    refused(cluster.recover(0, &["--resume", "orders/0", "--resume-full", "orders/1"]));
    assert_eq!([snapshot(&replaced), snapshot(&quarantined)], before);
    fs::write(replaced.join("CONFIGURATION"), &marker).unwrap();

    let selections = after_crash(&cluster, policy);
    processes.restart_recovering(&cluster, 0, 3, &selections);
    processes.ready(0).await;
    finish_resume(&cluster, &mut processes, client, &configurations).await;
}

async fn seed(cluster: &Cluster, processes: &mut Processes) -> (Client, bytes::Bytes) {
    let checked = cluster.checked(0);
    let mut client = processes
        .observe("open recovery SDK", Client::open(&checked))
        .await;
    let pending = client.queue(0).await;
    processes
        .observe("confirm small recovery cohort", client.confirm(pending))
        .await;
    // Several transfer windows leave a physical prefix we can observe before
    // publication. Opaque distinct bodies avoid compressing this into one window.
    let pending = client.queue_large(10, 512).await;
    let probe = pending[0].2.slice(..64);
    processes
        .observe(
            "confirm multi-window recovery cohort",
            client.confirm(pending),
        )
        .await;
    processes
        .observe("verify donor history", client.replay())
        .await;
    processes.shutdown().await;
    (client, probe)
}

async fn receive_and_interrupt(
    processes: &mut Processes,
    replaced: &Path,
    marker: &[u8],
    probe: &[u8],
) {
    processes
        .observe("receive a physical transfer prefix", async {
            loop {
                if snapshot(&replaced.join("segments"))
                    .values()
                    .any(|bytes| bytes.windows(probe.len()).any(|window| window == probe))
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
    // Freeze the actual process, then check the durable publication boundary.
    // Seeing bytes alone never proves that interruption preceded publication.
    processes.signal(0, "STOP");
    assert_eq!(
        fs::read(replaced.join("CONFIGURATION")).unwrap(),
        marker,
        "transfer published before the process was frozen"
    );
    processes.stop(0, "KILL").await;
}

fn after_crash(cluster: &Cluster, policy: Confirmation) -> Vec<&'static str> {
    let mut selections = vec!["--resume", "orders/0"];
    // An unclean memory-voting partition requires explicit nonvoting recovery
    // even when its payload files look intact. Disk-quorum neighbors can restart.
    for (number, name) in [(1, "orders/1"), (2, "orders/2"), (3, "orders/3")] {
        if number != 1 && policy == Confirmation::DiskQuorum {
            continue;
        }
        let configuration = fs::read(cluster.partition(0, number).join("CONFIGURATION")).unwrap();
        let flag = if configuration.starts_with(b"OZYRECOV") {
            "--resume-full"
        } else {
            "--quarantine"
        };
        selections.extend([flag, name]);
    }
    selections
}

async fn finish_resume(
    cluster: &Cluster,
    processes: &mut Processes,
    mut client: Client,
    configurations: &[Vec<u8>; 4],
) {
    let replaced = cluster.partition(0, 0);
    let quarantined = cluster.partition(0, 1);
    processes
        .observe("publish every recovered partition", async {
            loop {
                if configurations.iter().enumerate().all(|(number, expected)| {
                    fs::read(cluster.partition(0, number as u32).join("CONFIGURATION"))
                        .is_ok_and(|actual| &actual == expected)
                }) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
    let pending = client.queue(1).await;
    processes
        .observe("confirm after resumed transfer", client.confirm(pending))
        .await;
    processes
        .observe("verify complete resumed history", client.replay())
        .await;
    assert_eq!(client.links.socket_count(), 6);
    processes
        .observe("close recovery SDK", client.close())
        .await;
    processes.shutdown().await;
    let before = [snapshot(&replaced), snapshot(&quarantined)];
    refused(cluster.recover(0, &["--resume", "orders/0"]));
    refused(cluster.recover(0, &["--resume-full", "orders/1"]));
    assert_eq!([snapshot(&replaced), snapshot(&quarantined)], before);
    processes.restart(cluster, 0, 4);
    processes.ready(0).await;
    processes.stop(0, "TERM").await;
}
