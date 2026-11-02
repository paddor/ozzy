//! Held records that come due while their leader is cut off from its peers.
//! The cut-off leader keeps running and keeps its client links but cannot
//! commit a release, so each record leaves once, through the next leader.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_held_records_due_while_the_leader_is_cut_off_leave_once() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mode = DeploymentMode::Three;
        let clock = SdkClock::manual();
        let cluster = delay_cluster(mode, &clock, 64).await;
        let client = progress(
            &clock,
            Client::open_with_clock(&cluster.configs[0].0, &cluster.runtime, clock.clone()),
        )
        .await;
        let mut writer = writer(&clock, &client).await;
        let arrivals = arrivals(&clock, &client, 9).await;
        // Leadership can still move until the first write confirms.
        let mut sent = send(&clock, &mut writer, [(1, None, record(1, None))]).await;
        let now = broker_now(&clock);
        // Due after the cut, out of send order.
        let held = (2..=9_u8).map(|id| {
            let due = Some(now + 3_000 + u64::from((id * 5) % 8) * 400);
            (id, due, record(id, due))
        });
        sent.extend(send(&clock, &mut writer, held).await);
        let leader = leader(&clock, &cluster, &client).await;
        cluster.isolate(leader, true);
        let last = sent.iter().filter_map(|sent| sent.due).max().unwrap();
        wait_until(&clock, last + 2_000).await;
        cluster.isolate(leader, false);
        let records = progress(&clock, arrivals).await.unwrap();
        // A repeated release would take the next offset first.
        let receipts = confirm(&clock, &mut writer, [record(u8::MAX, None)]).await;
        assert_eq!(receipts, [(AppendDisposition::Stored, sent.len() as u64)]);
        check_releases(mode, &sent, &records);
        drop(writer);
        progress(&clock, client.close()).await;
        progress(&clock, cluster.shutdown()).await;
    })
    .await
    .expect("held records across a cut-off leader made no bounded progress");
}
