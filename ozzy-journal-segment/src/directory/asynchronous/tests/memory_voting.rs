use super::*;

#[test]
fn drained_evidence_requires_exact_progress_configuration_and_completed_shutdown() {
    let (mut controller, io) = setup(baseline());
    let mut journal = drive(&mut controller, open(io, 2)).unwrap();
    assert!(drive(&mut controller, journal.require_drained_memory_history()).is_err());
    drive(&mut controller, journal.publish_drained_memory_history()).unwrap();
    drive(&mut controller, journal.require_drained_memory_history()).unwrap();
    let old_configuration = journal.configuration.replace(b"other deployment".to_vec());
    assert!(drive(&mut controller, journal.require_drained_memory_history()).is_err());
    journal.configuration = old_configuration;
    drive(&mut controller, journal.mark_memory_voting_running()).unwrap();
    assert!(drive(&mut controller, journal.require_drained_memory_history()).is_err());
    drive(&mut controller, append_confirmed(&mut journal));
    assert!(drive(&mut controller, journal.require_drained_memory_history()).is_err());
    drive(&mut controller, journal.publish_drained_memory_history()).unwrap();
    drop(journal);
    let (mut controller, io) = setup(controller.crash(true).unwrap().0);
    let journal = drive(&mut controller, open(io, 3)).unwrap();
    drive(&mut controller, journal.require_drained_memory_history()).unwrap();
    assert_eq!(journal.accepted_position().unwrap().op_number, 2);
}

#[test]
fn running_publication_must_be_durable_before_memory_votes_are_enabled() {
    let (mut controller, io) = setup(baseline());
    let mut journal = drive(&mut controller, open(io, 2)).unwrap();
    drive(&mut controller, journal.publish_drained_memory_history()).unwrap();
    drop(journal);
    let image = controller.crash(true).unwrap().0;
    for immediate in [false, true] {
        let mut finished = false;
        for cut in 0..150 {
            let (mut controller, io) = setup(image.clone());
            let mut journal = drive(&mut controller, open(io, 3)).unwrap();
            let done = {
                let mut future = std::pin::pin!(journal.mark_memory_voting_running());
                let mut done = false;
                for _ in 0..cut {
                    if let Poll::Ready(result) = poll(future.as_mut()) {
                        result.unwrap();
                        done = true;
                        break;
                    }
                    let (id, stage) = controller.jobs()[0];
                    match stage {
                        Stage::Queued => {
                            controller.execute(id, Effect::Normal).unwrap();
                            if immediate {
                                controller.deliver(id).unwrap();
                            }
                        }
                        Stage::Executed => controller.deliver(id).unwrap(),
                    }
                }
                done
            };
            if cut != 0 && !done {
                assert!(journal.is_faulted());
            }
            drop(journal);
            let (mut controller, io) = setup(controller.crash(true).unwrap().0);
            let journal = drive(&mut controller, open(io, 4)).unwrap();
            let drained = drive(&mut controller, journal.require_drained_memory_history()).is_ok();
            assert!(
                !done || !drained,
                "completed running marker must prevent intact restart"
            );
            if done {
                finished = true;
                break;
            }
        }
        assert!(finished);
    }
}

#[test]
fn failed_marker_barrier_fences_even_if_the_marker_reached_storage() {
    for after in [false, true] {
        let (mut controller, io) = setup(baseline());
        let mut journal = drive(&mut controller, open(io, 2)).unwrap();
        let result = drive_with(
            &mut controller,
            journal.mark_memory_voting_running(),
            |operation| {
                if matches!(operation, Operation::Sync { .. }) {
                    if after {
                        Effect::FailAfter(io::ErrorKind::Other)
                    } else {
                        Effect::FailBefore(io::ErrorKind::Other)
                    }
                } else {
                    Effect::Normal
                }
            },
        );
        assert!(result.is_err());
        assert!(journal.is_faulted());
        assert!(drive(&mut controller, journal.publish_drained_memory_history()).is_err());
    }
}

#[test]
fn every_corrupted_byte_missing_file_and_temporary_clean_file_fail_closed() {
    use crate::directory::memory_voting::BYTES;
    let path = "/group/MEMORY_VOTING";
    let (mut controller, io) = setup(baseline());
    let mut journal = drive(&mut controller, open(io, 2)).unwrap();
    drive(&mut controller, journal.publish_drained_memory_history()).unwrap();
    let original = controller
        .image()
        .bytes(Path::new(path), false)
        .unwrap()
        .to_vec();
    assert_eq!(original.len(), BYTES);
    for index in 0..BYTES {
        let mut bytes = original.clone();
        bytes[index] ^= 1;
        write_file(&mut controller, &journal.access, path, &bytes);
        assert!(
            drive(&mut controller, journal.require_drained_memory_history()).is_err(),
            "byte {index}"
        );
    }
    write_file(&mut controller, &journal.access, path, &original);
    drive(&mut controller, journal.require_drained_memory_history()).unwrap();
    write_file(
        &mut controller,
        &journal.access,
        "/group/.MEMORY_VOTING.tmp",
        &original,
    );
    remove_file(&mut controller, &journal.access, path);
    assert!(drive(&mut controller, journal.require_drained_memory_history()).is_err());
    for bytes in [&original[..BYTES - 1], &[0; BYTES + 1][..]] {
        write_file(&mut controller, &journal.access, path, bytes);
        assert!(drive(&mut controller, journal.require_drained_memory_history()).is_err());
    }
}
