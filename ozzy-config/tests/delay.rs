#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    reason = "test code"
)]

use ozzy_config::{Deployment, TopicDelay};
use uuid::Uuid;

const SINGLE: &str = include_str!("fixtures/single.toml");

fn enabled() -> Deployment {
    Deployment::parse(&format!(
        "{SINGLE}\n[topics.orders.delay]\nmax_secs = 3600\n"
    ))
    .unwrap()
}

#[test]
fn delay_is_opt_in_with_a_fixed_maximum_and_a_default_held_record_limit() {
    let disabled = Deployment::parse(SINGLE).unwrap().validate().unwrap();
    assert!(disabled.deployment().topics["orders"].delay.is_none());
    let enabled = enabled().validate().unwrap();
    let delay = enabled.deployment().topics["orders"].delay.unwrap();
    assert_eq!(
        delay,
        TopicDelay {
            max_secs: 3600,
            max_entries: 100_000
        }
    );
    assert!(
        enabled
            .partition_placements("laptop")
            .unwrap()
            .iter()
            .all(|partition| partition.delay == Some(delay))
    );
    for fields in ["", "max_entries = 8", "max_secs = 1\nmax_delay = 1"] {
        assert!(
            Deployment::parse(&format!("{SINGLE}\n[topics.orders.delay]\n{fields}\n")).is_err()
        );
    }
}

#[test]
fn delay_rejects_zero_overflow_deduplication_and_a_delay_past_retention() {
    for case in 0..6 {
        let mut deployment = enabled();
        let topic = deployment.topics.get_mut("orders").unwrap();
        let delay = topic.delay.as_mut().unwrap();
        match case {
            0 => delay.max_secs = 0,
            1 => delay.max_secs = u64::MAX,
            2 => delay.max_entries = 0,
            3 => delay.max_entries = ozzy_config::MAX_DELAY_ENTRIES + 1,
            4 => topic.retention.max_age_secs = Some(3599),
            _ => {
                topic.deduplication = Some(ozzy_config::TopicDeduplication {
                    window_millis: 1000,
                    max_entries: 8192,
                    max_bytes: 8_388_608,
                });
            }
        }
        assert!(deployment.validate().is_err(), "accepted case {case}");
    }
    let mut deployment = enabled();
    deployment
        .topics
        .get_mut("orders")
        .unwrap()
        .retention
        .max_age_secs = Some(3600);
    assert!(deployment.validate().is_ok());
}

#[test]
fn provisioned_maximum_delay_is_fixed_while_the_held_record_limit_can_change() {
    let deployment = enabled().validate().unwrap();
    let mut next = 0_u128;
    let identity = deployment
        .initialize(|| {
            next += 1;
            Uuid::from_u128(next)
        })
        .unwrap();
    let restored = ozzy_config::DeploymentIdentity::decode(&identity.encode().unwrap()).unwrap();
    assert_eq!(restored, identity);
    assert_eq!(
        restored.topics["orders"].delay_max_millis.unwrap().get(),
        3_600_000
    );
    for change in 0..3 {
        let mut config = enabled();
        let delay = &mut config.topics.get_mut("orders").unwrap().delay;
        match change {
            0 => *delay = None,
            1 => delay.as_mut().unwrap().max_secs = 60,
            _ => delay.as_mut().unwrap().max_entries = 8,
        }
        let checked = config.validate().unwrap();
        assert_eq!(checked.check_identity(&restored).is_ok(), change == 2);
    }
}
