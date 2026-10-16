use super::*;
use ozzy_io::OpenMode;
use std::rc::Rc;

fn checkpoint(controller: &mut Controller, journal: &mut Journal) -> (CheckpointFiles, Vec<u8>) {
    journal.limits.checkpoint.max_chunk_bytes = 64 * 1024;
    let state: Vec<_> = (0..128 * 1024).map(|byte| (byte % 251) as u8).collect();
    drive(controller, append_confirmed(journal));
    drive(controller, commit(journal));
    let files = drive(
        controller,
        journal.build_checkpoint(id(), schema(), 64 * 1024, &state),
    )
    .unwrap();
    (files, state)
}

#[test]
fn small_checkpoint_ranges_do_not_retain_large_physical_chunk_backings() {
    let (mut controller, mut journal) = empty_journal();
    let (files, state) = checkpoint(&mut controller, &mut journal);
    for (offset, maximum) in [
        (0, 128),
        (4090, 32),
        (65520, 128),
        (65536, 17),
        (131_062, 32),
    ] {
        let bytes = drive_with(
            &mut controller,
            files.read_range(offset as u64, maximum),
            |operation| {
                if let Operation::Read { length, .. } = operation {
                    assert!(*length <= 4096);
                }
                Effect::Normal
            },
        )
        .unwrap();
        let count = maximum.min(65536 - offset % 65536);
        assert_eq!(bytes, state[offset..offset + count]);
        assert!(
            bytes.capacity() <= count,
            "small range retained {} bytes",
            bytes.capacity()
        );
    }
    assert!(drive(&mut controller, files.read_range(0, 0)).is_err());
    assert!(drive(&mut controller, files.read_range(state.len() as u64, 1)).is_err());
}

#[test]
fn checkpoint_range_detects_corruption_outside_the_requested_fragment() {
    for extra in [false, true] {
        let (mut controller, mut journal) = empty_journal();
        let (files, _) = checkpoint(&mut controller, &mut journal);
        let chunk = files.manifest().chunks[0];
        let path = journal
            .root()
            .join("checkpoints")
            .join(checkpoint_name(id()))
            .join(crate::checkpoint::chunk_name(chunk.ordinal));
        let file = drive(
            &mut controller,
            journal.access.open(path, OpenMode::ReadWrite, false, false),
        )
        .unwrap();
        drive(
            &mut controller,
            journal.access.write_all(
                &file,
                if extra { chunk.bytes } else { chunk.bytes - 1 },
                &[255],
            ),
        )
        .unwrap();
        drive(
            &mut controller,
            journal.access.done(Operation::Close { handle: file }),
        )
        .unwrap();
        let received = drive(&mut controller, files.read_range(0, 1)).unwrap_err();
        if extra {
            assert!(matches!(received, CheckpointError::LengthMismatch));
        } else {
            assert!(matches!(
                received,
                CheckpointError::DigestMismatch("checkpoint chunk")
            ));
        }
    }
}

#[test]
fn checkpoint_cursor_keeps_chunk_handles_and_reads_each_byte_once() {
    let (mut controller, mut journal) = empty_journal();
    let (files, state) = checkpoint(&mut controller, &mut journal);
    let mut cursor = Rc::new(files).cursor();
    let (mut opens, mut closes, mut read_bytes) = (0, 0, 0);
    let mut offset = 0;
    while offset < state.len() {
        let bytes = drive_with(
            &mut controller,
            cursor.read(offset as u64, 128),
            |operation| {
                match operation {
                    Operation::Open { .. } => opens += 1,
                    Operation::Close { .. } => closes += 1,
                    Operation::Read { length, .. } => {
                        assert!(*length <= 128);
                        read_bytes += length;
                    }
                    _ => {}
                }
                Effect::Normal
            },
        )
        .unwrap();
        assert_eq!(bytes, state[offset..offset + bytes.len()]);
        assert!(bytes.capacity() <= 128);
        offset += bytes.len();
        assert_eq!(cursor.is_verified(), offset == state.len());
    }
    assert_eq!((opens, closes, read_bytes), (2, 2, state.len()));
}

#[test]
fn checkpoint_cursor_replays_ranges_without_disturbing_sequential_progress() {
    let (mut controller, mut journal) = empty_journal();
    let (files, state) = checkpoint(&mut controller, &mut journal);
    let mut cursor = Rc::new(files).cursor();
    assert!(drive(&mut controller, cursor.read(0, 0)).is_err());
    assert!(drive(&mut controller, cursor.read(state.len() as u64, 1)).is_err());
    let mut offset = drive(&mut controller, cursor.read(0, 128)).unwrap().len();
    for (at, maximum) in [(0, 128), (0, 4096), (65530, 128), (131_062, 8192)] {
        let bytes = drive(&mut controller, cursor.read(at, maximum)).unwrap();
        assert!(bytes.len() <= 4096);
        assert_eq!(bytes, state[at as usize..at as usize + bytes.len()]);
        assert!(!cursor.is_verified());
    }
    while offset < state.len() {
        let bytes = drive(&mut controller, cursor.read(offset as u64, 8192)).unwrap();
        assert!(bytes.len() <= 4096);
        assert_eq!(bytes, state[offset..offset + bytes.len()]);
        offset += bytes.len();
    }
    assert!(cursor.is_verified());
}

#[test]
fn fresh_checkpoint_cursor_verifies_a_prefix_once_before_continuing() {
    let (mut controller, mut journal) = empty_journal();
    let (files, state) = checkpoint(&mut controller, &mut journal);
    let mut cursor = Rc::new(files).cursor();
    let mut read_bytes = 0;
    for offset in [65543, 65671] {
        let bytes = drive_with(&mut controller, cursor.read(offset, 128), |operation| {
            if let Operation::Read { length, .. } = operation {
                assert!(*length <= 4096);
                read_bytes += length;
            }
            Effect::Normal
        })
        .unwrap();
        assert_eq!(bytes, state[offset as usize..offset as usize + 128]);
        assert_eq!(read_bytes, offset as usize + 128);
    }
    assert!(!cursor.is_verified());
}

#[test]
fn checkpoint_cursor_detects_file_growth_after_returning_a_private_prefix() {
    let (mut controller, mut journal) = empty_journal();
    let (files, state) = checkpoint(&mut controller, &mut journal);
    let files = Rc::new(files);
    let mut cursor = files.clone().cursor();
    let bytes = drive(&mut controller, cursor.read(0, 4096)).unwrap();
    assert_eq!(bytes, state[..4096]);
    let chunk = files.manifest().chunks[0];
    let file = drive(
        &mut controller,
        journal.access.open(
            journal
                .root()
                .join("checkpoints")
                .join(checkpoint_name(id()))
                .join(crate::checkpoint::chunk_name(chunk.ordinal)),
            OpenMode::ReadWrite,
            false,
            false,
        ),
    )
    .unwrap();
    drive(
        &mut controller,
        journal.access.write_all(&file, chunk.bytes, &[255]),
    )
    .unwrap();
    drive(
        &mut controller,
        journal.access.done(Operation::Close { handle: file }),
    )
    .unwrap();
    for offset in (4096..chunk.bytes).step_by(4096) {
        let received = drive(&mut controller, cursor.read(offset, 4096));
        if offset + 4096 == chunk.bytes {
            assert!(matches!(received, Err(CheckpointError::LengthMismatch)));
        } else {
            assert_eq!(
                received.unwrap(),
                state[offset as usize..offset as usize + 4096]
            );
        }
    }
    assert!(!cursor.is_verified());
    assert!(drive(&mut controller, cursor.read(0, 128)).is_err());
}

#[test]
fn canceled_checkpoint_cursor_requires_a_fresh_cursor() {
    let (mut controller, mut journal) = empty_journal();
    let (files, state) = checkpoint(&mut controller, &mut journal);
    let files = Rc::new(files);
    let mut cursor = files.clone().cursor();
    let mut reading = Box::pin(cursor.read(0, 128));
    let held = loop {
        assert!(poll(reading.as_mut()).is_pending());
        let jobs = controller.jobs();
        if let Some((id, _)) = jobs.iter().find(|(id, _)| {
            matches!(controller.operation(*id), Some(Operation::Protected { operation, .. })
                if matches!(&**operation, Operation::Read { .. }))
        }) {
            break *id;
        }
        for (id, _) in jobs {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
    };
    drop(reading);
    assert!(drive(&mut controller, cursor.read(0, 128)).is_err());
    drop(cursor);
    controller.execute(held, Effect::Normal).unwrap();
    controller.deliver(held).unwrap();
    let mut fresh = files.cursor();
    assert_eq!(
        drive(&mut controller, fresh.read(0, 128)).unwrap(),
        state[..128]
    );
}
