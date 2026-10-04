use super::*;
use crate::process::Fixture;

#[tokio::test(flavor = "current_thread")]
#[ignore = "single-broker disk soak; OZZY_SOAK_SECONDS and RESULTS select the run"]
async fn single_broker_varied_io_soak() {
    let seconds = duration_secs();
    let fixture = Fixture::new().keep_artifacts();
    fixture.provision_volumes();
    fixture.format();
    let mut broker = fixture.start_on_cpu(0, Some(0));
    broker.ready().await;
    let mut client = broker
        .observe("open single soak SDK", Client::open(&fixture.checked()))
        .await;
    let mut reader = broker
        .observe("open single soak reader", client.reader(false))
        .await;
    let mut ledger = File::create(std::env::var("OZZY_SOAK_RESULTS").unwrap()).unwrap();
    let start = Instant::now();
    let (mut wave, mut verified, mut restarts) = (0, 0, 0);
    let mut next_restart = Duration::from_secs(60);
    let mut next_resume = Duration::from_secs(30);
    let mut next_report = Duration::ZERO;
    while start.elapsed() < Duration::from_secs(seconds) {
        if start.elapsed() >= next_restart {
            let killed = restarts % 4 != 3;
            broker.signal(if killed { "KILL" } else { "TERM" });
            broker.exited(killed).await;
            restarts += 1;
            broker = fixture.start_on_cpu(restarts, Some(0));
            broker.ready().await;
            next_restart = start.elapsed() + Duration::from_secs(90);
        }
        if start.elapsed() >= next_resume {
            client = broker
                .observe(
                    "resume single soak producer",
                    client.reopen_producer(restarts % 2 == 1),
                )
                .await;
            next_resume = start.elapsed() + Duration::from_secs(30);
        }
        let positions = client.positions();
        let pending = broker
            .observe("admit single soak records", client.queue_varied(wave))
            .await;
        let records = pending.len();
        broker
            .observe("confirm single soak records", client.confirm(pending))
            .await;
        if wave % 6 == 2 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        broker
            .observe(
                "verify single soak live reader",
                client.read(&mut reader, positions),
            )
            .await;
        client.discard_verified();
        verified += records;
        wave += 1;
        if start.elapsed() >= next_report {
            writeln!(
                ledger,
                "{{\"elapsed_secs\":{},\"verified_records\":{verified},\"restarts\":{restarts}}}",
                start.elapsed().as_secs()
            )
            .unwrap();
            ledger.flush().unwrap();
            next_report = start.elapsed() + Duration::from_secs(60);
        }
        tokio::time::sleep(Duration::from_millis(if wave % 6 == 0 { 150 } else { 10 })).await;
    }
    broker
        .observe("close single soak reader", reader.close())
        .await
        .unwrap();
    broker
        .observe("close single soak SDK", client.close())
        .await;
    broker.signal("TERM");
    broker.exited(false).await;
    writeln!(ledger, "{{\"complete\":true,\"elapsed_secs\":{},\"verified_records\":{verified},\"restarts\":{restarts}}}", start.elapsed().as_secs()).unwrap();
}
