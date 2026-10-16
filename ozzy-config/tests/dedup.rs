#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    reason = "test code"
)]

use ozzy_config::{Deployment, DeploymentIdentity};
use uuid::Uuid;

const SINGLE: &str = include_str!("fixtures/single.toml");

fn enabled() -> Deployment {
    Deployment::parse(&format!(
        "{SINGLE}\n[topics.orders.deduplication]\nmax_entries = 8192\nmax_bytes = 8388608\n"
    ))
    .unwrap()
}

#[test]
fn deduplication_is_opt_in_with_a_five_minute_window_and_explicit_capacity() {
    let disabled = Deployment::parse(SINGLE).unwrap().validate().unwrap();
    assert!(
        disabled.deployment().topics["orders"]
            .deduplication
            .is_none()
    );
    let enabled = enabled().validate().unwrap();
    let policy = enabled.deployment().topics["orders"].deduplication.unwrap();
    assert_eq!(policy.window_millis, 300_000);
    assert_eq!(policy.max_entries, 8192);
    assert_eq!(policy.max_bytes, 8_388_608);
    assert!(
        enabled
            .partition_placements("laptop")
            .unwrap()
            .iter()
            .all(|partition| { partition.deduplication == Some(policy) })
    );
    for bounds in [
        "",
        "max_entries = 8192",
        "max_bytes = 8388608",
        "windwo_millis = 1",
    ] {
        assert!(
            Deployment::parse(&format!(
                "{SINGLE}\n[topics.orders.deduplication]\n{bounds}\n"
            ))
            .is_err()
        );
    }
}

#[test]
fn deduplication_bounds_reject_zero_overflow_and_impossible_identity_storage() {
    for field in 0..6 {
        let mut deployment = enabled();
        let policy = deployment
            .topics
            .get_mut("orders")
            .unwrap()
            .deduplication
            .as_mut()
            .unwrap();
        match field {
            0 => policy.window_millis = 0,
            1 => policy.max_entries = 0,
            2 => policy.max_bytes = 0,
            3 => policy.max_entries = u64::MAX,
            4 => policy.max_bytes = u64::MAX,
            _ => policy.max_bytes = policy.max_entries * 32 - 1,
        }
        assert!(
            deployment.validate().is_err(),
            "accepted invalid field {field}"
        );
    }
}

#[test]
fn provisioned_dedup_window_is_fixed_while_resource_limits_can_change() {
    let deployment = enabled().validate().unwrap();
    let mut next = 0_u128;
    let identity = deployment
        .initialize(|| {
            next += 1;
            Uuid::from_u128(next)
        })
        .unwrap();
    let encoded = identity.encode().unwrap();
    let restored = DeploymentIdentity::decode(&encoded).unwrap();
    assert_eq!(restored, identity);
    assert_eq!(
        restored.topics["orders"].dedup_window_millis.unwrap().get(),
        300_000
    );
    for change in 0..4 {
        let mut config = enabled();
        let policy = &mut config.topics.get_mut("orders").unwrap().deduplication;
        match change {
            0 => *policy = None,
            1 => policy.as_mut().unwrap().window_millis = 1,
            2 => policy.as_mut().unwrap().max_entries *= 2,
            _ => policy.as_mut().unwrap().max_bytes *= 2,
        }
        let checked = config.validate().unwrap();
        assert_eq!(checked.check_identity(&restored).is_ok(), change >= 2);
    }
    let disabled = Deployment::parse(SINGLE).unwrap().validate().unwrap();
    let mut next = 0_u128;
    let identity = disabled
        .initialize(|| {
            next += 1;
            Uuid::from_u128(next)
        })
        .unwrap();
    assert!(deployment.check_identity(&identity).is_err());
}
