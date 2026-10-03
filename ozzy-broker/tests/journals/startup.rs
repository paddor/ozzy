use super::*;
use futures::{StreamExt, stream::FuturesUnordered};
use ozzy_broker::{ApplicationShards, DevicePools};
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn native_journals_format_and_recover_on_symmetric_shards_with_shared_devices() {
    for policy in [Confirmation::DiskQuorum, Confirmation::ReplicatedPersisting] {
        let temporary = tempfile::tempdir().unwrap();
        let brokers = fixture(temporary.path(), DeploymentMode::Three, policy, 6);
        for (checked, local, journals) in brokers {
            storage(&checked, &local);
            for format in [true, false] {
                let (devices, lanes) = DevicePools::start(&checked.plan).unwrap();
                let threads = Arc::new(Mutex::new(BTreeMap::new()));
                let factory = {
                    let threads = threads.clone();
                    let partitions = journals.partitions.clone();
                    move |mut context: ozzy_broker::ShardContext| {
                        let plans: Vec<_> = partitions
                            .iter()
                            .filter(|part| part.placement.shard == context.plan.id)
                            .cloned()
                            .collect();
                        let threads = threads.clone();
                        async move {
                            let mut starting = FuturesUnordered::new();
                            for plan in plans {
                                let io = context.io.clone();
                                starting.push(async move {
                                    if format {
                                        Box::pin(plan.format(io, JournalGeneration(1))).await
                                    } else {
                                        Box::pin(plan.open(io, JournalGeneration(2))).await
                                    }
                                });
                            }
                            let mut owners = Vec::new();
                            while let Some(result) = starting.next().await {
                                let opened = result?;
                                let PartitionAuthority::Replicated(startup) = opened.authority
                                else {
                                    panic!("replicated group downgraded");
                                };
                                assert_eq!(startup.recovered().is_none(), format);
                                assert_eq!(opened.journal.images().is_ok(), format);
                                threads.lock().unwrap().insert(
                                    opened.placement.partition,
                                    (context.plan.id, std::thread::current().id()),
                                );
                                owners.push(opened.journal);
                            }
                            context.ready()?;
                            context.shutdown.requested().await;
                            for owner in owners {
                                owner.shutdown().await.unwrap();
                            }
                            Ok(())
                        }
                    }
                };
                let shards = ApplicationShards::start(&checked.plan, lanes, factory)
                    .await
                    .unwrap();
                assert_eq!(shards.thread_count(), checked.plan.shards.len());
                {
                    let observed = threads.lock().unwrap();
                    assert_eq!(observed.len(), 6);
                    for placement in &checked.plan.partitions {
                        assert_eq!(observed[&placement.partition].0, placement.shard);
                    }
                }
                shards.shutdown().await.unwrap();
                devices.shutdown().await;
            }
        }
    }
}
