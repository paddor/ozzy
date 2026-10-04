//! Actual SDK, broker and inproc owners; all file operations remain in memory.

use super::*;
use crate::process::Client as SdkClient;
use ozzy_broker::{RecoveryIntent, RecoverySelection};
use std::{fs::File, io::Write, path::PathBuf, time::Instant};

mod cluster;
mod host;
use cluster::MemoryCluster;

#[tokio::test(flavor = "current_thread")]
async fn varied_inproc_churn_preserves_payloads_across_short_writes_and_recovery() {
    for policy in [
        Confirmation::LocalDurable,
        Confirmation::DiskQuorum,
        Confirmation::ReplicatedPersisting,
    ] {
        churn(policy, None, None).await;
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "long memory-only real-broker soak; OZZY_SOAK_POLICY, SECONDS and RESULTS select the run"]
async fn memory_only_varied_churn_soak() {
    let policy = match std::env::var("OZZY_SOAK_POLICY").unwrap().as_str() {
        "single" => Confirmation::LocalDurable,
        "dq" => Confirmation::DiskQuorum,
        "rp" => Confirmation::ReplicatedPersisting,
        value => panic!("unknown soak policy {value}"),
    };
    let seconds: u64 = std::env::var("OZZY_SOAK_SECONDS")
        .unwrap_or_else(|_| "3600".into())
        .parse()
        .unwrap();
    assert!(seconds > 0);
    let ledger = File::create(std::env::var("OZZY_SOAK_RESULTS").unwrap()).unwrap();
    churn(policy, Some(Duration::from_secs(seconds)), Some(ledger)).await;
}

async fn churn(policy: Confirmation, duration: Option<Duration>, mut ledger: Option<File>) {
    let mut cluster = MemoryCluster::new(policy).await;
    let mut client = SdkClient::open_with_runtime(&cluster.configs[0].0, &cluster.runtime).await;
    let mut reader = live_many(&cluster.brokers, "open churn reader", client.reader(false)).await;
    let start = Instant::now();
    let (mut wave, mut verified, mut restarts) = (0, 0, 0);
    let mut next_restart = Duration::from_secs(30);
    let mut next_report = Duration::ZERO;
    while duration.map_or(wave < 18, |duration| start.elapsed() < duration) {
        verified += cluster.verify_wave(&mut client, &mut reader, wave).await;
        client.discard_verified();
        if wave % 17 == 0 {
            client = live_many(
                &cluster.brokers,
                "resume memory churn",
                client.reopen_producer(wave % 34 == 0),
            )
            .await;
        }
        if start.elapsed() >= next_restart || (duration.is_none() && wave == 12) {
            let index = restarts % cluster.brokers.len();
            cluster.restart(index).await;
            cluster.wait_recovered(index).await;
            restarts += 1;
            next_restart = start.elapsed() + Duration::from_secs(30);
        }
        if start.elapsed() >= next_report {
            report(
                &mut ledger,
                policy,
                start.elapsed(),
                verified,
                restarts,
                cluster.faults,
                false,
            );
            next_report = start.elapsed() + Duration::from_secs(60);
        }
        wave += 1;
        if duration.is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    live_many(&cluster.brokers, "close churn reader", reader.close())
        .await
        .unwrap();
    live_many(&cluster.brokers, "close churn SDK", client.close()).await;
    let faults = cluster.faults;
    cluster.shutdown().await;
    report(
        &mut ledger,
        policy,
        start.elapsed(),
        verified,
        restarts,
        faults,
        true,
    );
}

fn report(
    ledger: &mut Option<File>,
    policy: Confirmation,
    elapsed: Duration,
    verified: usize,
    restarts: usize,
    faults: usize,
    complete: bool,
) {
    if let Some(ledger) = ledger {
        let record = format!(
            "{{\"policy\":\"{policy:?}\",\"elapsed_secs\":{},\"verified_records\":{verified},\"scheduled_restarts\":{restarts},\"short_write_recoveries\":{faults},\"complete\":{complete}}}",
            elapsed.as_secs()
        );
        writeln!(ledger, "{record}").unwrap();
        ledger.flush().unwrap();
        println!("{record}");
    }
}
