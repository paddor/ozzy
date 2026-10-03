use super::*;
use serde_json::json;

#[test]
fn control_can_flatten_into_a_parser_also_named_args() {
    use clap::{CommandFactory, Parser};
    #[derive(Parser)]
    struct Args {
        #[command(flatten)]
        control: super::Args,
    }
    Args::command().debug_assert();
    let parsed = Args::try_parse_from(["bench", "--control-bind", "tcp://127.0.0.1:0"]).unwrap();
    assert_eq!(
        parsed.control.control_bind.as_deref(),
        Some("tcp://127.0.0.1:0")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn abstract_control_roundtrips_chunked_reports_and_shutdown() {
    run(&Args::default(), 1, async {
        let (mut coordinator, descriptor) = Connection::listen(None).await?;
        assert!(
            descriptor
                .control_endpoint
                .as_ref()
                .unwrap()
                .starts_with("ipc://@ozzy-bench-")
        );
        let mut worker = Connection::connect(&context(), &descriptor).await?;
        let report = json!({"event":"reported", "payload":"x".repeat(CHUNK * 2 + 17)});
        worker.send(&report)?;
        assert_eq!(
            tokio::time::timeout(DEADLINE, coordinator.receive()).await??,
            report
        );
        coordinator.send(&json!({"command":"finish"}))?;
        assert_eq!(worker.receive().await?["command"], "finish");
        let mut input = worker.take_input()?;
        coordinator.shutdown()?;
        assert!(
            tokio::time::timeout(DEADLINE, input.recv())
                .await?
                .is_none()
        );
        worker.finish().await?;
        Ok(())
    })
    .await
    .unwrap();
}

#[test]
fn framing_rejects_wrong_run_peer_and_oversized_chunks() {
    let run = Uuid::now_v7();
    let worker = Uuid::now_v7();
    let message = Route {
        run,
        worker,
        peer: CONTROLLER,
    }
    .packet(Kind::Data, 1, 0, 2, b"{}");
    assert!(
        Frame::decode(
            &message,
            Route {
                run,
                worker,
                peer: CONTROLLER
            }
        )
        .is_ok()
    );
    assert!(
        Frame::decode(
            &message,
            Route {
                run: Uuid::now_v7(),
                worker,
                peer: CONTROLLER
            }
        )
        .is_err()
    );
    assert!(
        Frame::decode(
            &message,
            Route {
                run,
                worker: Uuid::now_v7(),
                peer: CONTROLLER
            }
        )
        .is_err()
    );
    assert!(
        Frame::decode(
            &message,
            Route {
                run,
                worker,
                peer: [0; 16]
            }
        )
        .is_err()
    );
    let message = Route {
        run,
        worker,
        peer: CONTROLLER,
    }
    .packet(Kind::Data, 1, 0, MAX_REPORT_BYTES + 1, b"{}");
    assert!(
        Frame::decode(
            &message,
            Route {
                run,
                worker,
                peer: CONTROLLER
            }
        )
        .is_err()
    );
}

#[test]
fn control_admission_is_bounded_and_closed_is_reported() {
    let (sender, receiver) = mpsc::channel(QUEUE);
    for _ in 0..QUEUE {
        enqueue(&sender, Kind::Data, Value::Null).unwrap();
    }
    assert!(enqueue(&sender, Kind::Data, Value::Null).is_err());
    drop(receiver);
    assert!(enqueue(&sender, Kind::Data, Value::Null).is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn controller_death_closes_worker_input() {
    run(&Args::default(), 1, async {
        let (mut coordinator, descriptor) = Connection::listen(None).await?;
        let mut worker = Connection::connect(&context(), &descriptor).await?;
        worker.send(&json!({"event":"ready"}))?;
        assert_eq!(coordinator.receive().await?["event"], "ready");
        drop(coordinator);
        assert!(
            tokio::time::timeout(Duration::from_secs(3), worker.receive())
                .await?
                .is_err()
        );
        assert!(!worker.monitor().is_live());
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn premature_worker_exit_is_not_successful_completion() {
    run(&Args::default(), 1, async {
        let (mut coordinator, descriptor) = Connection::listen(None).await?;
        let worker = Connection::connect(&context(), &descriptor).await?;
        worker.send(&json!({"event":"ready"}))?;
        assert_eq!(coordinator.receive().await?["event"], "ready");
        drop(worker);
        assert!(
            tokio::time::timeout(Duration::from_secs(3), coordinator.receive())
                .await?
                .is_err()
        );
        assert!(!coordinator.monitor().is_live());
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn worker_failure_cause_survives_acknowledged_disconnect() {
    run(&Args::default(), 1, async {
        let (mut coordinator, descriptor) = Connection::listen(None).await?;
        let monitor = coordinator.monitor();
        let work = tokio::spawn(async move {
            run(&descriptor, 1, async {
                Err::<(), _>(bench_error("retained-history budget exhausted"))
            })
            .await
        });
        let cause = tokio::time::timeout(Duration::from_secs(3), coordinator.receive())
            .await?
            .unwrap_err();
        assert!(
            cause
                .to_string()
                .contains("retained-history budget exhausted")
        );
        assert!(work.await?.is_err());
        // The subsequent transport disconnect must not replace the cause.
        tokio::time::timeout(Duration::from_secs(3), async {
            while monitor.is_live() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(
            monitor
                .failure()
                .unwrap()
                .contains("retained-history budget exhausted")
        );
        Ok(())
    })
    .await
    .unwrap();
}
