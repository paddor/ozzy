use super::*;
use crate::directory::evidence::{COPY_STRIDE, Copies, FILE_BYTES, RECORD_BYTES, decode};

const DURABLE: &str = "/group/DURABLE";

fn durable(controller: &Controller) -> Vec<u8> {
    controller
        .image()
        .bytes(Path::new(DURABLE), false)
        .unwrap()
        .to_vec()
}

fn confirmed(count: usize) -> Image {
    let (mut controller, mut journal) = empty_journal();
    for _ in 0..count {
        drive(&mut controller, append_confirmed(&mut journal));
    }
    drop(journal);
    controller.crash(true).unwrap().0
}

#[test]
fn repeated_confirmations_keep_metadata_files_constant_and_protect_the_latest_tail() {
    let (mut controller, mut journal) = empty_journal();
    let generation = journal.current().generation;
    let names = |controller: &mut Controller, journal: &Journal| {
        drive(controller, journal.access.list("/group".into(), 128, 16384))
            .unwrap()
            .len()
    };
    let count = names(&mut controller, &journal);
    for _ in 0..6 {
        drive(&mut controller, append_confirmed(&mut journal));
        assert_eq!(journal.current().generation, generation);
        assert_eq!(names(&mut controller, &journal), count);
    }
    let copies = Copies::decode_file(&durable(&controller), &journal.manifest).unwrap();
    assert_eq!(copies.protected(&journal.manifest).unwrap().op_number, 6);
    let end = journal.writer.durable_position().end_offset();
    drop(journal);

    let (mut controller, io) = setup(controller.crash(true).unwrap().0);
    let access = raw_access(&io);
    let segment = drive(
        &mut controller,
        access.open(
            "/group/segments/1.log".into(),
            ozzy_io::OpenMode::ReadWrite,
            false,
            false,
        ),
    )
    .unwrap();
    set_length(&mut controller, &access, &segment, end - 1);
    drive(&mut controller, access.sync(&segment)).unwrap();
    assert!(drive(&mut controller, open(io, 2)).is_err());
}

#[test]
fn damaged_evidence_never_falls_back_to_the_manifest_or_a_temporary() {
    let image = confirmed(1);
    // Case 5 is the undamaged control: the leftover temporary alone is harmless.
    for damage in 0..6 {
        let (mut controller, io) = setup(image.clone());
        let access = raw_access(&io);
        let intact = durable(&controller);
        write_file(&mut controller, &access, "/group/.DURABLE.tmp", &intact);
        let mut bytes = intact.clone();
        match damage {
            0 => {
                bytes[80] ^= 1;
                bytes[COPY_STRIDE + 80] ^= 1;
            }
            1 => bytes.truncate(FILE_BYTES - 1),
            2 => bytes.fill(0),
            3 => remove_file(&mut controller, &access, DURABLE),
            4 => bytes.push(0),
            5 => {}
            _ => unreachable!(),
        }
        if damage != 3 {
            write_file(&mut controller, &access, DURABLE, &bytes);
        }
        assert_eq!(
            drive(&mut controller, open(io, 2)).is_ok(),
            damage == 5,
            "damage {damage}"
        );
    }
}

#[test]
fn one_damaged_copy_is_tolerated_and_rewritten_at_recovery() {
    let image = confirmed(3);
    for damaged in 0..2 {
        let (mut controller, io) = setup(image.clone());
        let intact = durable(&controller);
        assert_eq!(
            intact[..RECORD_BYTES],
            intact[COPY_STRIDE..][..RECORD_BYTES]
        );
        let mut bytes = intact.clone();
        bytes[damaged * COPY_STRIDE + 80] ^= 1;
        write_file(&mut controller, &raw_access(&io), DURABLE, &bytes);
        let journal = drive(&mut controller, open(io, 2)).unwrap();
        assert_eq!(journal.accepted_position().unwrap().op_number, 3);
        drop(journal);
        let image = controller.crash(true).unwrap().0;
        assert_eq!(
            image.bytes(Path::new(DURABLE), true).unwrap(),
            intact,
            "damaged copy {damaged}"
        );
    }
}

#[test]
fn publication_never_overwrites_the_newest_copy_first() {
    let (mut controller, mut journal) = empty_journal();
    for id in 1..=4 {
        let op = operation(&journal);
        let written = drive(&mut controller, journal.append(&[op], BodyEncoding::Raw)).unwrap();
        drive(&mut controller, journal.sync_through(written)).unwrap();
        let manifest = journal.manifest.clone();
        let before = durable(&controller);
        let copies = Copies::decode_file(&before, &manifest).unwrap();
        let (bytes, first) = copies
            .next(&manifest, journal.accepted_position().unwrap())
            .unwrap();
        // Tear the first write: the other copy must still decode the prior prefix.
        let mut torn = before.clone();
        torn[first * COPY_STRIDE..][..RECORD_BYTES / 2].copy_from_slice(&bytes[..RECORD_BYTES / 2]);
        assert_eq!(
            Copies::decode_file(&torn, &manifest)
                .unwrap()
                .protected(&manifest)
                .unwrap(),
            copies.protected(&manifest).unwrap()
        );
        drive(&mut controller, journal.publish_durable_progress()).unwrap();
        let after = Copies::decode_file(&durable(&controller), &manifest).unwrap();
        assert!(after.mirrored());
        assert_eq!(after.protected(&manifest).unwrap().op_number, id);
    }
}

#[test]
fn evidence_remains_binding_across_metadata_changes_and_rejects_future_scope() {
    let (mut controller, mut journal) = empty_journal();
    drive(&mut controller, append_confirmed(&mut journal));
    let bytes = durable(&controller)[..RECORD_BYTES].to_vec();
    let manifest = journal.manifest.clone();
    for change_volume in [false, true] {
        let mut other = manifest.clone();
        if change_volume {
            other.identity.volume_id = VolumeId::from_bytes([99; 16]);
        } else {
            other.identity.replica_node_id = NodeId::from_bytes([99; 16]);
        }
        assert!(decode(&bytes, &other).is_err());
    }
    let mut next = journal.next_manifest().unwrap();
    next.promised_view += 1;
    assert_eq!(decode(&bytes, &next).unwrap().op_number, 1);
    let mut earlier = next.clone();
    earlier.generation = 0;
    assert!(decode(&bytes, &earlier).is_err());
    next.segments.last_mut().unwrap().segment_id += 1;
    assert_eq!(decode(&bytes, &next).unwrap(), next.accepted);
    next.generation -= 1;
    assert!(decode(&bytes, &next).is_err());
}
