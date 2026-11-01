//! A resent duplicate reads the one operation that holds its proof.

use super::*;

fn stored_batch(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    first: u64,
) {
    let validated = ready(produce(
        controller,
        journal,
        driver,
        [request(12, first, 1)],
        0,
    ));
    accept(controller, journal, driver, validated);
}

/// A second producer's copy of the record at offset `first`, at `sequence`.
fn copy(sequence: u64, first: u64) -> crate::replica_journal::ProducerAppend<'static> {
    let mut copy = request(30, sequence, 1);
    copy.records[0] = request(12, first, 1).records[0].clone();
    copy
}

fn duplicate_batch(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    sequence: u64,
    first: u64,
) {
    let validated = ready(produce(
        controller,
        journal,
        driver,
        [copy(sequence, first)],
        0,
    ));
    assert_eq!(outcomes(&validated), [vec![duplicate(first)]]);
    accept(controller, journal, driver, validated);
}

fn roll(controller: &mut Controller, journal: &mut OwnedJournal) {
    let work = journal.begin_roll(8).unwrap();
    let done = drive(controller, work.publish());
    journal.complete_roll(done).unwrap();
    ready_spare(controller, journal);
}

/// Resend the duplicate batch and name every file it opened.
fn resend(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
    sequence: u64,
    first: u64,
) -> Vec<String> {
    let ticket = driver.begin_validation().unwrap();
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .prepare_stream_append(
            ozzy_proto::append::Authority {
                group_id: ticket.scope().group_id,
                config_epoch: ticket.scope().configuration_epoch,
                view: ticket.scope().view,
            },
            copy(sequence, first),
        )
        .unwrap();
    let mut opened = Vec::new();
    let result = drive_except(
        controller,
        journal.propose_append(ticket, buffer, 0),
        None,
        |operation| {
            if let Operation::Open { path, .. } = operation {
                opened.push(path.display().to_string());
            }
            Effect::Normal
        },
    );
    let ProposalValidation::Resolved { buffer, .. } = result else {
        panic!("exact retry: {result:?}")
    };
    assert_eq!(
        buffer
            .producer_retry_results()
            .iter()
            .map(|span| span.first_offset.get())
            .collect::<Vec<_>>(),
        [first]
    );
    opened
}

fn segment_of(path: &str) -> &str {
    let name = path.rsplit('/').next().unwrap();
    name.split(['.', '-']).next().unwrap()
}

#[tokio::test(start_paused = true)]
async fn a_resent_duplicate_opens_only_the_segment_that_holds_its_proof() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(limits());
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config.clone(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    ready_spare(&mut controller, &mut journal);
    stored_batch(&mut controller, &mut journal, &mut driver, 0);
    duplicate_batch(&mut controller, &mut journal, &mut driver, 0, 0);
    roll(&mut controller, &mut journal);
    stored_batch(&mut controller, &mut journal, &mut driver, 1);
    roll(&mut controller, &mut journal);
    stored_batch(&mut controller, &mut journal, &mut driver, 2);
    duplicate_batch(&mut controller, &mut journal, &mut driver, 1, 2);
    roll(&mut controller, &mut journal);
    stored_batch(&mut controller, &mut journal, &mut driver, 3);
    duplicate_batch(&mut controller, &mut journal, &mut driver, 2, 3);
    drive(&mut controller, journal.shutdown()).unwrap();

    // A new owner holds selectors for its active segment only.
    let (mut journal, driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(2)),
    )
    .unwrap();
    let active = resend(&mut controller, &mut journal, &driver, 2, 3);
    assert!(
        active.iter().all(|path| segment_of(path) == "4"),
        "active: {active:?}"
    );
    let newest = resend(&mut controller, &mut journal, &driver, 1, 2);
    assert!(
        newest.iter().all(|path| segment_of(path) == "3"),
        "newest sealed: {newest:?}"
    );
    assert!(
        newest.iter().any(|path| path.contains("indexes/")),
        "its first use loads the predecessor: {newest:?}"
    );
    let again = resend(&mut controller, &mut journal, &driver, 1, 2);
    assert!(
        !again.iter().any(|path| path.contains("indexes/")),
        "a retry burst loads it once: {again:?}"
    );
    let oldest = resend(&mut controller, &mut journal, &driver, 0, 0);
    assert!(
        oldest.iter().all(|path| segment_of(path) == "1"),
        "older sealed: {oldest:?}"
    );
    drive(&mut controller, journal.shutdown()).unwrap();
}
