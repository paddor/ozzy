//! Test-only TCP broker process with the same controlled memory file backend.

use super::*;

#[tokio::test(flavor = "current_thread")]
#[ignore = "launched by the host soak with OZZY_MEMORY_CONFIG, BROKER and RECOVERING"]
async fn memory_only_broker_process() {
    let name = std::env::var("OZZY_MEMORY_BROKER").unwrap();
    let config = PathBuf::from(std::env::var("OZZY_MEMORY_CONFIG").unwrap());
    let checked = ozzy_broker::check_config(
        ozzy_broker::load_deployment(&config).unwrap(),
        Path::new("shared.identity"),
        &name,
        &ozzy_broker::host_resources().unwrap(),
    )
    .unwrap();
    let local = ozzy_broker::load_broker_identity(&checked, Path::new("local.identity")).unwrap();
    let recovering = std::env::var("OZZY_MEMORY_RECOVERING").unwrap() == "1";
    let selections: Vec<_> = if recovering {
        (0..4)
            .map(|partition| RecoverySelection {
                topic: "orders".into(),
                partition,
                intent: RecoveryIntent::Quarantine,
            })
            .collect()
    } else {
        vec![]
    };
    let runtime = WriterRuntime::new().unwrap();
    let image = Box::pin(provision(&checked, &local, true)).await;
    let (broker, _, image) = start_image_selected(
        runtime.context().clone(),
        checked,
        local,
        true,
        image,
        &selections,
    )
    .await;
    println!("Serving broker {name} with test memory storage");
    let mut terminate =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    tokio::select! {
        result = broker.closed() => panic!("memory broker exited: {result:?}"),
        _ = terminate.recv() => {},
    }
    broker.shutdown().await.unwrap();
    image.await.unwrap();
}
