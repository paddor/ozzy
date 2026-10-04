use super::clock::progress;
use super::*;
use ozzy_runtime::replicated::SdkClock;

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_device_cuts_with_held_completions_preserve_retry_identity() {
    tokio::time::timeout(Duration::from_secs(120), async {
        for policy in [
            Confirmation::LocalDurable,
            Confirmation::DiskQuorum,
            Confirmation::ReplicatedPersisting,
        ] {
            for power_loss in [false, true] {
                let clock = SdkClock::manual();
                let mut cluster = progress(
                    &clock,
                    broker::Cluster::configured(policy, Some(clock.clone()), |_| {}),
                )
                .await;
                let mut client = progress(
                    &clock,
                    Client::open_with_clock(&cluster.configs[0].0, &cluster.runtime, clock.clone()),
                )
                .await;
                let pending = progress(&clock, client.queue(0)).await;
                progress(&clock, client.confirm(pending)).await;
                let primary = client.leader(0);
                let index = cluster
                    .configs
                    .iter()
                    .position(|(config, _)| {
                        ozzy_proto::NodeId::from_bytes(
                            *config.identity.brokers[&config.plan.name].as_bytes(),
                        ) == primary
                    })
                    .unwrap();
                let control = cluster.controls[index].clone();
                control.hold_completions(true);
                let pending = progress(&clock, client.queue(1)).await;
                progress(&clock, async {
                    while control.pending_completions() == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await;
                progress(&clock, cluster.crash(index, power_loss)).await;
                progress(&clock, client.confirm(pending)).await;
                progress(&clock, cluster.wait_recovered(index)).await;
                progress(&clock, client.replay()).await;
                let pending = progress(&clock, client.queue(2)).await;
                progress(&clock, client.confirm(pending)).await;
                progress(&clock, client.replay()).await;
                progress(&clock, client.close()).await;
                progress(&clock, cluster.shutdown()).await;
            }
        }
    })
    .await
    .expect("physical device cut prevented retry or recovery progress");
}
