use super::*;
use ozzy_config::{ByteRateLimit, ConfigError};

#[test]
fn independent_network_limits_reach_the_selected_broker_plan() {
    for (ingress, egress) in [(true, false), (false, true), (true, true)] {
        let mut source = format!("{SINGLE}\n[brokers.laptop.topology.omq]\nio_threads = 2\n");
        if ingress {
            source.push_str("ingress = { bytes_per_second = 1000000, burst_bytes = 64000 }\n");
        }
        if egress {
            source.push_str("egress = { bytes_per_second = 2000000, burst_bytes = 128000 }\n");
        }
        let plan = Deployment::parse(&source)
            .unwrap()
            .validate()
            .unwrap()
            .broker_plan("laptop", &host())
            .unwrap();
        assert_eq!(plan.omq_io_threads, 2);
        assert_eq!(
            plan.omq_ingress,
            ingress.then_some(ByteRateLimit {
                bytes_per_second: 1_000_000,
                burst_bytes: 64_000,
            })
        );
        assert_eq!(
            plan.omq_egress,
            egress.then_some(ByteRateLimit {
                bytes_per_second: 2_000_000,
                burst_bytes: 128_000,
            })
        );
    }
}

#[test]
fn zero_network_rate_or_burst_names_the_invalid_direction() {
    for direction in ["ingress", "egress"] {
        for fields in [
            "bytes_per_second = 0\nburst_bytes = 1",
            "bytes_per_second = 1\nburst_bytes = 0",
        ] {
            let source = format!("{SINGLE}\n[brokers.laptop.topology.omq.{direction}]\n{fields}\n");
            let error = Deployment::parse(&source).unwrap().validate().unwrap_err();
            assert!(matches!(
                error,
                ConfigError::Invalid { path, .. }
                if path == format!("brokers.laptop.topology.omq.{direction}")
            ));
        }
    }
}

#[test]
fn network_limits_reject_incomplete_negative_and_unknown_fields() {
    for direction in ["ingress", "egress"] {
        for fields in [
            "",
            "bytes_per_second = 1",
            "burst_bytes = 1",
            "bytes_per_second = -1\nburst_bytes = 1",
            "bytes_per_second = 1\nburst_bytes = -1",
            "bytes_per_second = 1\nburst_bytes = 1\nextra = 2",
            "bytes_per_second = 1.5\nburst_bytes = 1",
        ] {
            let source = format!("{SINGLE}\n[brokers.laptop.topology.omq.{direction}]\n{fields}\n");
            assert!(
                Deployment::parse(&source).is_err(),
                "accepted {direction}: {fields}"
            );
        }
    }
}

#[test]
fn network_limits_are_local_to_each_broker() {
    let mut config = three();
    for (index, broker) in config.brokers.values_mut().enumerate() {
        broker.topology.omq.ingress = Some(ByteRateLimit {
            bytes_per_second: index as u64 + 1,
            burst_bytes: 1,
        });
    }
    let validated = config.validate().unwrap();
    for (index, name) in ["a", "b", "c"].into_iter().enumerate() {
        let plan = validated.broker_plan(name, &host()).unwrap();
        assert_eq!(plan.omq_ingress.unwrap().bytes_per_second, index as u64 + 1);
        assert_eq!(plan.omq_egress, None);
    }
}

#[test]
fn changing_network_limits_preserves_persisted_identity() {
    let original = single().validate().unwrap();
    let identity = original.initialize(ids()).unwrap();
    let mut changed = single();
    let omq = &mut changed.brokers.get_mut("laptop").unwrap().topology.omq;
    omq.ingress = Some(ByteRateLimit {
        bytes_per_second: 100,
        burst_bytes: 10,
    });
    omq.egress = Some(ByteRateLimit {
        bytes_per_second: 200,
        burst_bytes: 20,
    });
    let changed = changed.validate().unwrap();
    changed.check_identity(&identity).unwrap();
    assert_eq!(changed.initialize(ids()).unwrap(), identity);
}
