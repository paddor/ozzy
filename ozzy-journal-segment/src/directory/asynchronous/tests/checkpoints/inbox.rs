use super::*;
use ozzy_io::OpenMode;
use ozzy_journal::integrity::IntegrityHasher;

fn digest(bytes: &[u8]) -> Digest {
    let mut hash = IntegrityHasher::new(crate::checkpoint::CHECKPOINT_STATE_HASH_CONTEXT);
    hash.update(bytes);
    hash.finish()
}

fn path(journal: &Journal) -> PathBuf {
    journal.root().join("staging").join(format!(
        ".checkpoint-{}-{}-0.tmp",
        checkpoint_name(id()),
        journal.manifest().generation,
    ))
}

#[test]
fn checkpoint_inbox_streams_bounded_fragments_without_selecting_history() {
    let (mut controller, journal) = empty_journal();
    let before = journal.manifest().clone();
    let bytes: Vec<u8> = (0..12289).map(|n| n as u8).collect();
    let mut inbox = drive(
        &mut controller,
        journal.begin_checkpoint_receive(id(), bytes.len() as u64, digest(&bytes)),
    )
    .unwrap();
    let retained = inbox.retained_bytes();
    assert!(retained < 4096);
    assert!(inbox.stream().is_err());
    assert!(drive(&mut controller, inbox.push(1, &bytes[..32])).is_err());
    assert!(drive(&mut controller, inbox.push(0, &bytes[..4097])).is_err());
    assert_eq!(inbox.through(), 0);
    for fragment in bytes.chunks(inbox.fragment_bytes()) {
        drive(&mut controller, inbox.push(inbox.through(), fragment)).unwrap();
        assert_eq!(inbox.retained_bytes(), retained);
    }
    assert!(inbox.is_complete());
    assert_eq!(journal.manifest(), &before);
    let mut stream = inbox.stream().unwrap();
    assert!(!stream.is_verified());
    let mut read = Vec::new();
    while let Some(fragment) = drive(&mut controller, stream.next(511)).unwrap() {
        assert!(fragment.len() <= 511);
        read.extend_from_slice(&fragment);
    }
    assert!(stream.is_verified());
    assert_eq!(read, bytes);
    let staging = path(&journal);
    drive(&mut controller, inbox.discard()).unwrap();
    assert!(controller.image().bytes(&staging, false).is_err());
    assert_eq!(journal.manifest(), &before);
}

#[test]
fn checkpoint_inbox_rechecks_physical_length_and_digest_on_each_scan() {
    for extra in [false, true] {
        let (mut controller, journal) = empty_journal();
        let bytes = [19; 512];
        let mut inbox = drive(
            &mut controller,
            journal.begin_checkpoint_receive(id(), 512, digest(&bytes)),
        )
        .unwrap();
        drive(&mut controller, inbox.push(0, &bytes)).unwrap();
        let file = drive(
            &mut controller,
            journal
                .access
                .open(path(&journal), OpenMode::ReadWrite, false, false),
        )
        .unwrap();
        drive(
            &mut controller,
            journal
                .access
                .write_all(&file, if extra { 512 } else { 0 }, &[20]),
        )
        .unwrap();
        let mut stream = inbox.stream().unwrap();
        let error = drive(&mut controller, stream.next(4096)).unwrap_err();
        if extra {
            assert!(matches!(error, CheckpointError::LengthMismatch));
        } else {
            assert!(matches!(error, CheckpointError::DigestMismatch(_)));
        }
        assert!(!stream.is_verified());
        assert!(drive(&mut controller, stream.next(1)).is_err());
        drive(&mut controller, inbox.discard()).unwrap();
    }
}

#[test]
fn checkpoint_inbox_rejects_wrong_input_hash_and_open_cleans_private_staging() {
    let (mut controller, io) = setup(baseline());
    let journal = drive(&mut controller, open(io.clone(), 2)).unwrap();
    let staging = path(&journal);
    let mut inbox = drive(
        &mut controller,
        journal.begin_checkpoint_receive(id(), 32, digest(&[1; 32])),
    )
    .unwrap();
    assert!(matches!(
        drive(&mut controller, inbox.push(0, &[2; 32])),
        Err(CheckpointError::DigestMismatch(_))
    ));
    assert!(!inbox.is_complete());
    assert!(inbox.stream().is_err());
    assert!(drive(&mut controller, inbox.push(0, &[1; 32])).is_err());
    drop(inbox);
    drop(journal);
    let journal = drive(&mut controller, open(io, 3)).unwrap();
    assert!(controller.image().bytes(&staging, false).is_err());
    assert!(journal.manifest().checkpoint.is_none());
}

#[test]
fn canceled_checkpoint_inbox_write_keeps_jobs_fenced_until_late_completion() {
    for executed in [false, true] {
        let (mut controller, io) = setup(baseline());
        let journal = drive(&mut controller, open(io.clone(), 2)).unwrap();
        let staging = path(&journal);
        let mut inbox = drive(
            &mut controller,
            journal.begin_checkpoint_receive(id(), 32, digest(&[1; 32])),
        )
        .unwrap();
        let mut write = Box::pin(inbox.push(0, &[1; 32]));
        assert!(poll(write.as_mut()).is_pending());
        let held = controller.jobs()[0].0;
        if executed {
            controller.execute(held, Effect::Normal).unwrap();
        }
        drop(write);
        assert_eq!(inbox.through(), 0);
        assert!(!inbox.is_complete());
        assert!(inbox.stream().is_err());
        assert!(drive(&mut controller, inbox.push(0, &[1; 32])).is_err());
        drop(inbox);
        drop(journal);
        // Unexecuted physical work owns the protected root and handle. An
        // already executed write can release them before reply observation.
        let reopened = {
            let mut future = Box::pin(open(io.clone(), 3));
            let mut result = None;
            for _ in 0..1000 {
                if let Poll::Ready(value) = poll(future.as_mut()) {
                    result = Some(value);
                    break;
                }
                for (job, stage) in controller.jobs() {
                    if job != held {
                        assert_eq!(stage, Stage::Queued);
                        controller.execute(job, Effect::Normal).unwrap();
                        controller.deliver(job).unwrap();
                    }
                }
            }
            result.expect("protected reopen must finish without the old job")
        };
        if executed {
            let journal = reopened.unwrap();
            assert!(controller.image().bytes(&staging, false).is_err());
            drop(journal);
        } else {
            assert!(
                reopened.is_err(),
                "queued write keeps the old root protected"
            );
        }
        if !executed {
            controller.execute(held, Effect::Normal).unwrap();
        }
        controller.deliver(held).unwrap();
        let journal = drive(&mut controller, open(io, 4)).unwrap();
        assert!(controller.image().bytes(&staging, false).is_err());
        assert!(journal.manifest().checkpoint.is_none());
    }
}

#[test]
fn canceled_checkpoint_inbox_scan_is_poisoned_but_a_fresh_scan_verifies() {
    let (mut controller, journal) = empty_journal();
    let mut inbox = drive(
        &mut controller,
        journal.begin_checkpoint_receive(id(), 512, digest(&[1; 512])),
    )
    .unwrap();
    drive(&mut controller, inbox.push(0, &[1; 512])).unwrap();
    let mut stream = inbox.stream().unwrap();
    let mut read = Box::pin(stream.next(32));
    assert!(poll(read.as_mut()).is_pending());
    let held = controller.jobs()[0].0;
    drop(read);
    assert!(!stream.is_verified());
    assert!(drive(&mut controller, stream.next(32)).is_err());
    controller.execute(held, Effect::Normal).unwrap();
    controller.deliver(held).unwrap();
    let mut fresh = inbox.stream().unwrap();
    assert_eq!(
        drive(&mut controller, fresh.next(512)).unwrap(),
        Some(vec![1; 512])
    );
    assert!(fresh.is_verified());
    drive(&mut controller, inbox.discard()).unwrap();
}
