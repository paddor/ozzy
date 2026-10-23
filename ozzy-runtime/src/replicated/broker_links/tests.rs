use super::*;
use ozzy_proto::{EnvelopeLimits, data::DataLimits};

/// Three brokers whose endpoints are never bound: each dial retries every
/// 100 ms.
fn unanswered() -> BrokerLinksConfig {
    let data = DataLimits {
        envelope: EnvelopeLimits {
            max_metadata_bytes: 16 * 1024,
            max_payload_bytes: 1024,
        },
        max_records: 4,
        max_parts: 4,
        max_record_bytes: 1024,
    };
    let parameters = handshake::Parameters::streaming(data, handshake::PRODUCER).unwrap();
    let name = ozzy_proto::RequestId::new();
    BrokerLinksConfig {
        local: NodeId::from_bytes([1; 16]),
        brokers: (0..3_u8)
            .map(|index| BrokerAddress {
                node: NodeId::from_bytes([index + 2; 16]),
                endpoint: format!("inproc://unanswered-{name}-{index}")
                    .parse()
                    .unwrap(),
                data_endpoint: format!("inproc://unanswered-data-{name}-{index}")
                    .parse()
                    .unwrap(),
            })
            .collect(),
        parameters,
        requests: 4,
        control_bytes: 1024 * 1024,
        routing_bytes: 1024 * 1024,
        append: None,
        reader: None,
        maximum_partitions: 1,
        request_timeout: Duration::from_millis(200),
        retry_interval: Duration::from_millis(10),
        clock: SdkClock::default(),
    }
}

#[tokio::test]
async fn links_survive_an_owner_stall_during_reconnects() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let runtime = WriterRuntime::new().unwrap();
        let links = BrokerLinks::connect(&runtime, unanswered()).await.unwrap();
        // Hold the owner thread across several dial retries to every broker.
        runtime
            .on_owner(|| async { std::thread::sleep(Duration::from_millis(700)) })
            .await
            .unwrap();
        assert!(matches!(
            links.topic("orders").await,
            Err(BrokerLinkError::Timeout)
        ));
        links.shutdown().await.unwrap();
    })
    .await
    .expect("links stalled after an owner stall");
}
