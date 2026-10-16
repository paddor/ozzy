use super::*;

/// Drain what `driver` does at `now`, then check that nothing happens before
/// its next deadline and that polling at the deadline acts. Returns it.
fn next_timed_action(driver: &mut ReplicaDriver, now: Duration) -> Duration {
    while driver.poll(now).unwrap().is_some() {}
    let deadline = driver.next_deadline().expect("a live role has a timer");
    assert!(deadline > now, "{deadline:?} is not after {now:?}");
    let before = deadline.saturating_sub(Duration::from_micros(1));
    assert_eq!(
        driver.poll(before).unwrap(),
        None,
        "acted before {deadline:?}"
    );
    assert!(
        driver.poll(deadline).unwrap().is_some(),
        "idle at {deadline:?}"
    );
    deadline
}

#[test]
fn a_primary_heartbeats_at_each_deadline() {
    let mut primary = driver(normal(0));
    let mut now = Duration::ZERO;
    for _ in 0..3 {
        let deadline = next_timed_action(&mut primary, now);
        assert_eq!(deadline, now + timing().heartbeat);
        now = deadline;
    }
}

#[test]
fn a_silent_primary_times_out_a_backup_then_its_election_retries_on_time() {
    let mut backup = driver(normal(1));
    let timeout = next_timed_action(&mut backup, Duration::ZERO);
    assert_eq!(timeout, timing().primary_timeout);
    let mut now = timeout;
    for _ in 0..3 {
        now = next_timed_action(&mut backup, now);
    }
    assert!(now <= timeout + 3 * timing().retransmit);
}
