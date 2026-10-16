//! Repeat bounded seeded schedules with accelerated protocol time and real codecs.

use super::storage_schedule;
use std::{
    fs::File,
    io::Write,
    time::{Duration, Instant},
};

#[test]
#[ignore = "long seeded fault job; OZZY_CHURN_SEED, SECONDS and RESULTS select the run"]
fn long_running_accelerated_fault_churn() {
    let seconds: u64 = std::env::var("OZZY_CHURN_SECONDS")
        .unwrap_or_else(|_| "3600".into())
        .parse()
        .unwrap();
    let mut seed: u64 = std::env::var("OZZY_CHURN_SEED")
        .unwrap_or_else(|_| "1".into())
        .parse()
        .unwrap();
    assert!(seconds > 0 && seed > 0);
    let mut ledger = File::create(std::env::var("OZZY_CHURN_RESULTS").unwrap()).unwrap();
    let start = Instant::now();
    let mut next_report = Duration::ZERO;
    let mut cases = 0;
    while start.elapsed() < Duration::from_secs(seconds) {
        let events = storage_schedule::sequence(seed);
        storage_schedule::run_seed("OZZY_STORAGE", storage_schedule::run, seed, &events);
        cases += 1;
        if start.elapsed() >= next_report {
            writeln!(
                ledger,
                "{{\"elapsed_secs\":{},\"last_seed\":{seed},\"cases\":{cases}}}",
                start.elapsed().as_secs()
            )
            .unwrap();
            ledger.flush().unwrap();
            next_report = start.elapsed() + Duration::from_secs(60);
        }
        seed = seed.checked_add(1).unwrap();
    }
    writeln!(
        ledger,
        "{{\"complete\":true,\"elapsed_secs\":{},\"next_seed\":{seed},\"cases\":{cases}}}",
        start.elapsed().as_secs()
    )
    .unwrap();
}
